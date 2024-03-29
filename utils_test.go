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

			if tt.args.filename != "" {
				defer os.Remove(tt.args.filename)
			}

			_, err := getOrCreate(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("getOrCreate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
