package csvdb

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrEntryNotFound is returned when a requested key does not exist
	ErrEntryNotFound = errors.New("entry not found")
	// ErrBackendNotSet is returned when the backend is unset
	ErrBackendNotSet = errors.New("backend not set")
	// ErrExportIsActive is returned when a export is attempted to start while one is still running
	ErrExportIsActive = errors.New("cannot start export as export is still active. If this error is frequent, consider increasing your ExportInterval values")
	// ErrPurgeIsActive is returned when a purge is attempted to start while one is still running
	ErrPurgeIsActive = errors.New("cannot start purge as purge is still active. If this error is frequent, consider increasing your PurgeInterval values")
)

func New[T Entry](ctx context.Context, o Options, b Backend) (db *DB[T], err error) {
	var d DB[T]
	if d, err = makeDB[T](o, b); err != nil {
		return
	}

	d.ctx, d.cancel = context.WithCancel(ctx)
	go scan(d.ctx, d.asyncBackup, d.o.ExportInterval)
	go scan(d.ctx, d.asyncPurge, d.o.PurgeInterval)
	db = &d
	return
}

// makeDB will make a DB without initializing background jobs
func makeDB[T Entry](o Options, b Backend) (d DB[T], err error) {
	if err = o.Validate(); err != nil {
		return
	}

	o.fill()

	fullDir := path.Join(o.Dir, o.Name)
	if err = os.MkdirAll(fullDir, 0744); err != nil {
		return
	}

	d.o = o
	d.b = b
	return
}

type DB[T Entry] struct {
	mux  sync.RWMutex
	emux sync.Mutex
	pmux sync.Mutex

	o Options

	b Backend

	ctx    context.Context
	cancel func()
}

func (d *DB[T]) Get(w io.Writer, key string) (err error) {
	// TODO: Uncomment this when we implement a thread-safe downloader.
	// Currently, multiple readers can download the same file and cause
	// race conditions.
	// d.mux.RLock()
	// defer d.mux.RUnlock()

	d.mux.Lock()
	defer d.mux.Unlock()

	var f fs.File
	if f, err = d.getOrDownload(key); err != nil {
		return
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return
}

func (d *DB[T]) GetMerged(w io.Writer, keys ...string) (err error) {
	// TODO: Uncomment this when we implement a thread-safe downloader.
	// Currently, multiple readers can download the same file and cause
	// race conditions.
	// d.mux.RLock()
	// defer d.mux.RUnlock()

	d.mux.Lock()
	defer d.mux.Unlock()

	return d.getMergedFile(w, keys)
}

func (d *DB[T]) Append(key string, es ...T) (err error) {
	if len(es) == 0 {
		return
	}

	d.mux.Lock()
	defer d.mux.Unlock()

	var (
		f        *os.File
		filename string
	)

	_, filename = d.getFilename(key)
	if f, err = getOrCreate(filename); err != nil {
		return
	}
	defer f.Close()
	return d.writeEntries(f, es)
}

func (d *DB[T]) AppendWithFunc(key string, fn func(*Rows) ([]T, error)) (err error) {
	d.mux.Lock()
	defer d.mux.Unlock()

	var (
		f        *os.File
		filename string
	)

	_, filename = d.getFilename(key)
	if f, err = getOrCreate(filename); err != nil {
		return
	}
	defer f.Close()

	var es []T
	r := makeRows(f)
	if es, err = fn(&r); err != nil {
		return
	}

	return d.writeEntries(f, es)
}

func (d *DB[T]) Delete(key string) (err error) {
	_, filename := d.getFilename(key)
	return os.Remove(filename)
}

func (d *DB[T]) Close() (err error) {
	d.cancel()
	return d.backup()
}

func (d *DB[T]) getOrDownload(key string) (f fs.File, err error) {
	name, filename := d.getFilename(key)
	f, err = os.Open(filename)
	switch {
	case err == nil:
		return
	case os.IsNotExist(err):
		return d.attemptDownload(name, filename)
	default:
		return
	}
}

func (d *DB[T]) getFilename(key string) (name, filename string) {
	name = fmt.Sprintf("%s.%s.csv", d.o.Name, key)
	filename = path.Join(d.getFullPath(), name)
	return
}

func (d *DB[T]) getFullPath() (fullPath string) {
	return path.Join(d.o.Dir, d.o.Name)
}

func (d *DB[T]) writeHeader(w *csv.Writer, created bool, e Entry) (err error) {
	if !created {
		return
	}

	return w.Write(e.Keys())
}

func (d *DB[T]) getMergedFile(w io.Writer, keys []string) (err error) {
	var headerWritten bool
	for _, key := range keys {
		var ok bool
		if ok, err = d.appendFile(w, !headerWritten, key); err != nil {
			return
		} else if ok {
			headerWritten = true
		}
	}

	return
}

func (d *DB[T]) appendFile(w io.Writer, writeHeader bool, key string) (ok bool, err error) {
	var f fs.File
	f, err = d.getOrDownload(key)
	switch err {
	case nil:
	case ErrEntryNotFound:
		err = nil
		return
	case ErrBackendNotSet:
		err = nil
		return
	default:
		return
	}

	fbuf := bufio.NewReader(f)
	if !writeHeader {
		if _, _, err = fbuf.ReadLine(); err != nil {
			return
		}
	}

	if _, err = io.Copy(w, fbuf); err != nil {
		return
	}

	ok = true
	return
}

func (d *DB[T]) attemptDownload(name, filename string) (f *os.File, err error) {
	if d.b == nil {
		err = ErrBackendNotSet
		return
	}

	if f, err = os.Create(filename); err != nil {
		return
	}

	if err = d.b.Import(context.Background(), d.o.Name, name, f); err == nil || !os.IsNotExist(err) {
		return
	}

	d.o.Logger.Printf("error downloading <%s>: %v\n", filename, err)
	if err := f.Close(); err != nil {
		fmt.Printf("csvdb.attemptDownload(): error closing empty file: %v\n", err)
	}

	if err := os.Remove(filename); err != nil {
		fmt.Printf("csvdb.attemptDownload(): error purging empty file: %v\n", err)
	}

	return
}

func (d *DB[T]) exportAll(exportable []string) (err error) {
	for _, name := range exportable {
		if err = d.export(name); err != nil {
			err = fmt.Errorf("error exporting <%s>: %v", name, err)
			return
		}
	}

	return
}

func (d *DB[T]) export(filename string) (err error) {
	if d.b == nil {
		err = ErrBackendNotSet
		return
	}

	var f *os.File
	filepath := path.Join(d.getFullPath(), filename)
	if f, err = os.Open(filepath); err != nil {
		err = fmt.Errorf("error opening <%s> for export: %v", filepath, err)
		return
	}
	defer f.Close()

	if _, err = d.b.Export(context.Background(), d.o.Name, filename, f); err != nil {
		return
	}

	return d.setLastExported(filename)
}

func (d *DB[T]) writeEntries(f *os.File, es []T) (err error) {
	if len(es) == 0 {
		return
	}

	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		return
	}

	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return
	}

	w := csv.NewWriter(f)
	isNew := info.Size() == 0
	if err = d.writeHeader(w, isNew, es[0]); err != nil {
		return
	}

	for _, e := range es {
		if err = w.Write(e.Values()); err != nil {
			return
		}
	}

	w.Flush()
	return
}

func (d *DB[T]) forEach(fn func(key string, info os.FileInfo) error) (err error) {
	dir := filepath.Join(d.o.Dir, d.o.Name)
	err = filepath.Walk(dir, func(path string, info fs.FileInfo, ierr error) (err error) {
		if ierr != nil {
			return ierr
		}

		if filepath.Dir(path) != dir {
			return
		}

		if filepath.Ext(path) != ".csv" {
			return
		}

		base := filepath.Base(path)

		return fn(base, info)
	})

	return
}

func (d *DB[T]) getExportable() (exportable []string, err error) {
	// TODO: Uncomment this when we implement a thread-safe downloader.
	// Currently, multiple readers can download the same file and cause
	// race conditions.
	// d.mux.RLock()
	// defer d.mux.RUnlock()

	d.mux.Lock()
	defer d.mux.Unlock()

	exportable = make([]string, 0, 32)
	err = d.forEach(func(key string, info fs.FileInfo) (err error) {
		lastExported := d.getLastExported(key)

		if lastExported.After(info.ModTime()) {
			// We exported since our last update, return
			return nil
		}

		exportable = append(exportable, info.Name())
		return
	})

	return
}

func (d *DB[T]) getExpired() (expired []string, err error) {
	// TODO: Uncomment this when we implement a thread-safe downloader.
	// Currently, multiple readers can download the same file and cause
	// race conditions.
	// d.mux.RLock()
	// defer d.mux.RUnlock()

	d.mux.Lock()
	defer d.mux.Unlock()

	expired = make([]string, 0, 32)
	err = d.forEach(func(key string, info fs.FileInfo) (err error) {

		if !d.o.ExpiryMonitor(key, info) {
			return
		}

		expired = append(expired, info.Name())
		return
	})

	return
}

func (d *DB[T]) removeAll(list []string) (err error) {
	d.mux.Lock()
	defer d.mux.Unlock()
	for _, filename := range list {
		filepath := path.Join(d.getFullPath(), filename)
		if err = os.Remove(filepath); err != nil {
			return
		}
	}

	return
}

func (d *DB[T]) purge() (err error) {
	if !d.pmux.TryLock() {
		return ErrPurgeIsActive
	}
	defer d.pmux.Unlock()

	var expired []string
	if expired, err = d.getExpired(); err != nil {
		return
	}

	return d.removeAll(expired)
}

func (d *DB[T]) asyncBackup() {
	if err := d.backup(); err != nil {
		d.o.Logger.Printf("csvdb.DB[%s].asyncBackup(): error exporting: %v\n", d.o.Name, err)
	}
}

func (d *DB[T]) asyncPurge() {
	if err := d.purge(); err != nil {
		d.o.Logger.Printf("csvdb.DB[%s].asyncPurge(): error purging: %v\n", d.o.Name, err)
	}
}

func (d *DB[T]) backup() (err error) {
	if !d.emux.TryLock() {
		return ErrExportIsActive
	}
	defer d.emux.Unlock()

	var exportable []string
	if exportable, err = d.getExportable(); err != nil {
		return
	}

	return d.exportAll(exportable)
}

func (d *DB[T]) setLastExported(name string) (err error) {
	var f *os.File
	filename := path.Join(d.getFullPath(), name)
	if f, err = os.Create(filename + ".exported"); err != nil {
		return
	}

	return f.Close()
}

func (d *DB[T]) getLastExported(name string) (t time.Time) {
	filename := path.Join(d.getFullPath(), name)
	exported, err := os.Stat(filename + ".exported")
	switch {
	case err == nil:
		return exported.ModTime()
	case os.IsNotExist(err):
		return
	default:
		fmt.Printf("csvdb[%s].getExportable() error getting filestat for exported file marker: %v\n", d.o.Name, err)
		return
	}
}
