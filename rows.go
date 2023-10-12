package csvdb

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
)

func makeRows(f *os.File) (r Rows) {
	r.f = f
	return
}

type Rows struct {
	mux sync.Mutex
	f   *os.File
}

func (r *Rows) ForEach(fn func([]string) error) (err error) {
	r.mux.Lock()
	defer r.mux.Unlock()

	if _, err = r.f.Seek(0, io.SeekStart); err != nil {
		return
	}

	rr := csv.NewReader(r.f)

	// Read past Header
	if _, err = rr.Read(); err != nil {
		return
	}

	var values []string
	for {
		if values, err = rr.Read(); err != nil {
			break
		}

		if err = fn(values); err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}
