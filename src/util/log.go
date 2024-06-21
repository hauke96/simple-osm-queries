package util

import "github.com/hauke96/sigolo/v2"

func LogFatalBug(format string, args ...interface{}) {
	sigolo.Fatalb(1, format+" - This is a bug, please report it at https://github.com/hauke96/sigolo/issues/new", args)
}
