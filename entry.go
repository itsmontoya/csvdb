package csvdb

type Entry interface {
	Keys() []string
	Values() []string
}
