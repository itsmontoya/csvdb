package csvdb

import (
	"testing"
	"time"
)

func TestOptions_Validate(t *testing.T) {
	type fields struct {
		Name    string
		Dir     string
		FileTTL time.Duration
	}

	type testcase struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []testcase{
		{
			name: "pass",
			fields: fields{
				Name:    "foo",
				Dir:     "bar",
				FileTTL: time.Hour,
			},
			wantErr: false,
		},
		{
			name: "fail - name",
			fields: fields{
				Name:    "",
				Dir:     "bar",
				FileTTL: time.Hour,
			},
			wantErr: true,
		},
		{
			name: "fail - dir",
			fields: fields{
				Name:    "foo",
				Dir:     "",
				FileTTL: time.Hour,
			},
			wantErr: true,
		},
		{
			name: "fail - fileTTL",
			fields: fields{
				Name:    "foo",
				Dir:     "bar",
				FileTTL: -time.Hour,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Options{
				Name:    tt.fields.Name,
				Dir:     tt.fields.Dir,
				FileTTL: tt.fields.FileTTL,
			}
			if err := o.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Options.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
