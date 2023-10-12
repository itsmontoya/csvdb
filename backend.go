package csvdb

import (
	"context"
	"io"
)

type Backend interface {
	Import(ctx context.Context, prefix, filename string, w io.Writer) (err error)
	Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error)
}
