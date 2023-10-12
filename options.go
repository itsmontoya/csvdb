package csvdb

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	ErrInvalidName      = errors.New("invalid name, cannot be empty")
	ErrInvalidDirectory = errors.New("invalid dir, cannot be empty")
	ErrInvalidFileTTL   = errors.New("invalid fileTTL, cannot be less than 0")
)

type Options struct {
	Name string `json:"name" toml:"name"`
	Dir  string `json:"dir" toml:"dir"`

	Logger Logger

	PurgeInterval time.Duration `json:"purgeInterval" toml:"purge-interval"`

	// FileTTL is the file duration all files
	// Note: This value is used to generate a basic ExpiryMonitor.
	// Both FileTTL and ExpiryMonitor are optional values, and only
	// one can be set at a time. ExpiryMonitor will always take priority
	FileTTL time.Duration `json:"fileTTL" toml:"file-ttl"`

	ExpiryMonitor ExpiryMonitor
}

func (o *Options) Validate() (err error) {
	var errs []error
	if len(o.Name) == 0 {
		errs = append(errs, ErrInvalidName)
	}

	if len(o.Dir) == 0 {
		errs = append(errs, ErrInvalidDirectory)
	}

	if o.FileTTL < 0 {
		errs = append(errs, ErrInvalidFileTTL)
	}

	return errors.Join(errs...)
}

func (o *Options) fill() {
	o.Dir = filepath.Clean(o.Dir)

	if o.ExpiryMonitor == nil {
		// Set default expiry monitor as a basic expiry monitor
		o.ExpiryMonitor = basicExpiryMonitor(o.FileTTL)
	}

	if o.PurgeInterval == 0 {
		// Set default purge interval for an hour
		o.PurgeInterval = time.Hour
	}

	if o.Logger == nil {
		o.Logger = log.New(os.Stdout, "csvdb", log.Ldate|log.Ltime)
	}
}

type ExpiryMonitor func(filename string, info os.FileInfo) (expired bool)
