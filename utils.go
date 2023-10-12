package csvdb

import (
	"io"
	"os"
	"time"
)

var openFile = os.OpenFile

func getOrCreate(filename string) (f *os.File, err error) {
	if f, err = openFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return
	}

	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return
	}

	return
}

func isExpiredBasic(ttl time.Duration, info os.FileInfo) (expired bool) {
	if ttl == 0 {
		return false
	}

	now := time.Now()
	return now.Sub(info.ModTime()) >= ttl
}

func basicExpiryMonitor(fileTTL time.Duration) ExpiryMonitor {
	return func(filepath string, info os.FileInfo) (expired bool) {
		return isExpiredBasic(fileTTL, info)
	}
}
