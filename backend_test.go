package csvdb

import (
	"context"
	"io"
)

var _ Backend = &mockBackend{}

type mockBackend struct {
	importFn func(ctx context.Context, prefix, filename string, w io.Writer) (err error)
	exportFn func(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error)
}

func (m *mockBackend) Import(ctx context.Context, prefix, filename string, w io.Writer) (err error) {
	if m.importFn == nil {
		return
	}

	return m.importFn(ctx, prefix, filename, w)
}

func (m *mockBackend) Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error) {
	if m.exportFn == nil {
		return filename, nil
	}

	return m.exportFn(ctx, prefix, filename, r)
}
