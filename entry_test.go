package csvdb

type testentry struct {
	Foo string
	Bar string
}

func (t testentry) Keys() []string {
	return []string{"foo", "bar"}
}

func (t testentry) Values() []string {
	return []string{t.Foo, t.Bar}
}
