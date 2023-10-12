package csvdb

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	type args struct {
		o Options
		b Backend
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				o: Options{
					Name:    "foo",
					Dir:     "./test",
					FileTTL: time.Hour * 24,
				},
			},
		},
		{
			name: "fail",
			args: args{
				o: Options{
					Name:    "",
					Dir:     "./test",
					FileTTL: time.Hour * 24,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New[testentry](tt.args.o, tt.args.b)
			defer os.RemoveAll(tt.args.o.Dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_Get(t *testing.T) {
	type args struct {
		key string
	}

	type testcase struct {
		name    string
		init    func() (*DB[testentry], error)
		args    args
		wantW   string
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Hour * 24 * 7

				b := &mockBackend{}
				if db, err = New[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = db.Append("foo", tvs...); err != nil {
					return
				}

				return
			},
			args: args{
				key: "foo",
			},
			wantW: `foo,bar
1,1b
2,2b
3,3b
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := tt.init()
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(d.o.Dir)

			w := &bytes.Buffer{}
			err = d.Get(w, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("DB.Get() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestDB_GetMerged(t *testing.T) {
	type args struct {
		keys []string
	}

	type testcase struct {
		name    string
		init    func() (*DB[testentry], error)
		args    args
		wantW   string
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Hour * 24 * 7

				b := &mockBackend{}
				if db, err = New[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = db.Append("1", tvs[0]); err != nil {
					return
				}

				if _, err = db.Append("2", tvs[1]); err != nil {
					return
				}

				if _, err = db.Append("3", tvs[2]); err != nil {
					return
				}

				return
			},
			args: args{
				keys: []string{"1", "2", "3"},
			},
			wantW: `foo,bar
1,1b
2,2b
3,3b
`,
			wantErr: false,
		},
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Hour * 24 * 7

				b := &mockBackend{}
				if db, err = New[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = db.Append("1", tvs[0]); err != nil {
					return
				}

				if _, err = db.Append("2", tvs[1]); err != nil {
					return
				}

				if _, err = db.Append("3", tvs[2]); err != nil {
					return
				}

				return
			},
			args: args{
				keys: []string{"1", "3"},
			},
			wantW: `foo,bar
1,1b
3,3b
`,
			wantErr: false,
		},
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Hour * 24 * 7

				b := &mockBackend{}
				if db, err = New[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = db.Append("1", tvs[0]); err != nil {
					return
				}

				if _, err = db.Append("2", tvs[1]); err != nil {
					return
				}

				if _, err = db.Append("3", tvs[2]); err != nil {
					return
				}

				return
			},
			args: args{
				keys: []string{"1"},
			},
			wantW: `foo,bar
1,1b
`,
			wantErr: false,
		},
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Hour * 24 * 7

				b := &mockBackend{}
				if db, err = New[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = db.Append("1", tvs[0]); err != nil {
					return
				}

				if _, err = db.Append("2", tvs[1]); err != nil {
					return
				}

				if _, err = db.Append("3", tvs[2]); err != nil {
					return
				}

				return
			},
			args: args{
				keys: []string{},
			},
			wantW:   ``,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := tt.init()
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(d.o.Dir)

			w := &bytes.Buffer{}
			err = d.GetMerged(w, tt.args.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("DB.Get() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestDB_purge(t *testing.T) {
	type testcase struct {
		name      string
		init      func() (*DB[testentry], error)
		wantCount int
		wantErr   bool
	}

	tests := []testcase{
		{
			name: "basic",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Millisecond

				b := &mockBackend{}
				var d DB[testentry]
				if d, err = makeDB[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = d.Append("foo", tvs...); err != nil {
					return
				}

				time.Sleep(time.Millisecond * 10)
				db = &d
				return
			},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name: "with remaining",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = time.Millisecond

				b := &mockBackend{}
				var d DB[testentry]
				if d, err = makeDB[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = d.Append("foo", tvs...); err != nil {
					return
				}

				time.Sleep(time.Millisecond * 10)

				if _, err = d.Append("bar", tvs...); err != nil {
					return
				}

				db = &d
				return
			},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name: "with no TTL",
			init: func() (db *DB[testentry], err error) {
				var opts Options
				opts.Dir = fmt.Sprintf("test_%d", time.Now().UnixNano())
				opts.Name = "foo"
				opts.FileTTL = 0

				b := &mockBackend{}
				var d DB[testentry]
				if d, err = makeDB[testentry](opts, b); err != nil {
					return
				}

				tvs := []testentry{
					{
						Foo: "1",
						Bar: "1b",
					},
					{
						Foo: "2",
						Bar: "2b",
					},
					{
						Foo: "3",
						Bar: "3b",
					},
				}

				if _, err = d.Append("foo", tvs...); err != nil {
					return
				}

				time.Sleep(time.Millisecond * 10)

				if _, err = d.Append("bar", tvs...); err != nil {
					return
				}

				db = &d
				return
			},
			wantCount: 2,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := tt.init()
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(d.o.Dir)

			err = d.purge()
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.purge() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var count int
			err = d.forEach(func(key string, info fs.FileInfo) (err error) {
				count++
				return
			})

			if err != nil {
				t.Fatal(err)
			}

			if count != tt.wantCount {
				t.Fatalf("DB.purge() count = %v, wantCount = %v", count, tt.wantCount)
			}
		})
	}
}
