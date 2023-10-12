package csvdb

import (
	"os"
	"time"
)

var openFile = os.OpenFile

func getOrCreate(filename string) (f *os.File, err error) {
	return openFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
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
