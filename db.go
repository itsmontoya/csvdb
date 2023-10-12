package csvdb

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
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
	// ErrPurgeIsActive is returned when a purge is attempted to start while one is still running
	ErrPurgeIsActive = errors.New("cannot start purge as purge is still active. If this error is frequent, consider increasing your PurgeInterval values")
)

func New[T Entry](o Options, b Backend) (db *DB[T], err error) {
	var d DB[T]
	if d, err = makeDB[T](o, b); err != nil {
		return
	}

	go d.scan()
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
	pmux sync.Mutex

	o Options

	b Backend
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

func (d *DB[T]) Append(key string, es ...T) (filename string, err error) {
	d.mux.Lock()
	defer d.mux.Unlock()

	if len(es) == 0 {
		return
	}

	var f *os.File
	filename = d.getFilename(key)
	if f, err = getOrCreate(filename); err != nil {
		return
	}
	defer f.Close()

	if err = d.writeEntries(f, es); err != nil {
		return
	}

	return d.export(filename, f)
}

func (d *DB[T]) Delete(key string) (err error) {
	filename := d.getFilename(key)
	return os.Remove(filename)
}

func (d *DB[T]) getOrDownload(key string) (f fs.File, err error) {
	filename := d.getFilename(key)
	f, err = os.Open(filename)
	switch {
	case err == nil:
		return
	case os.IsNotExist(err):
		return d.attemptDownload(filename)
	default:
		return
	}
}

func (d *DB[T]) getFilename(key string) (filename string) {
	return path.Join(d.getFullPath(), key+".csv")
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

func (d *DB[T]) attemptDownload(filename string) (f *os.File, err error) {
	if f, err = os.Create(filename); err != nil {
		return
	}

	if err = d.b.Import(context.Background(), d.o.Name, filename, f); err == nil {
		return
	}

	d.o.Logger.Printf("error downloading <%s>: %v\n", filename, err)

	if os.IsNotExist(err) {
		err = ErrEntryNotFound
		return
	}

	return
}

func (d *DB[T]) export(filename string, f *os.File) (createdFilename string, err error) {
	if _, err = f.Seek(0, 0); err != nil {
		return
	}

	return d.b.Export(context.Background(), d.o.Name, filename, f)
}

func (d *DB[T]) writeEntries(f *os.File, es []T) (err error) {
	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
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

func (d *DB[T]) scan() {
	ticker := time.NewTicker(d.o.PurgeInterval)
	for range ticker.C {
		go d.asyncPurge()
	}
}

func (d *DB[T]) asyncPurge() {
	if err := d.purge(); err != nil {
		d.o.Logger.Printf("csvdb.DB[%s].asyncPurge(): error purging: %v\n", d.o.Name, err)
	}
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
