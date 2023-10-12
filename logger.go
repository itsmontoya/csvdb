package csvdb

type Logger interface {
	Printf(format string, values ...any)
}
