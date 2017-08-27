package logiface

type Logger interface {
	Err(msg interface{}, keyvals ...interface{}) error
	PrintErr(msg interface{}, keyvals ...interface{})
	Warn(msg interface{}, keyvals ...interface{})
	Info(msg interface{}, keyvals ...interface{})
	Debug(msg interface{}, keyvals ...interface{})
	Panicf(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
}
