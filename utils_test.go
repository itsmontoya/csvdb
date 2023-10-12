package csvdb

import (
	"io/fs"
	"os"
	"testing"
)

func Test_getOrCreate(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name     string
		args     args
		openFile func(name string, flag int, perm fs.FileMode) (*os.File, error)
		wantErr  bool
	}{
		{
			name: "fail, bad name",
			args: args{
				filename: "",
			},
			wantErr: true,
		},
		{
			name: "fail, closed file",
			args: args{
				filename: "boop",
			},
			openFile: func(name string, flag int, perm fs.FileMode) (f *os.File, err error) {
				if f, err = os.OpenFile(name, flag, perm); err != nil {
					return
				}

				if err = f.Close(); err != nil {
					return
				}

				return
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.openFile != nil {
				orig := openFile
				defer func() {
					openFile = orig
				}()
				openFile = tt.openFile
			}

			_, err := getOrCreate(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("getOrCreate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
