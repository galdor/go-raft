package raft

import (
	"bytes"
	"fmt"
	"runtime"
)

func Panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func RecoverValueString(value interface{}) (msg string) {
	switch v := value.(type) {
	case error:
		msg = v.Error()
	case string:
		msg = v
	default:
		msg = fmt.Sprintf("%#v", v)
	}

	return
}

func StackTrace(depth int) string {
	pc := make([]uintptr, depth)

	// Always skip runtime.Callers and StackTrace
	nbFrames := runtime.Callers(2, pc)
	pc = pc[:nbFrames]

	var buf bytes.Buffer

	frames := runtime.CallersFrames(pc)
	for {
		frame, more := frames.Next()

		filePath := frame.File
		line := frame.Line
		function := frame.Function

		fmt.Fprintf(&buf, "%s\n", function)
		fmt.Fprintf(&buf, "  %s:%d\n", filePath, line)

		if !more {
			break
		}
	}

	return buf.String()
}
