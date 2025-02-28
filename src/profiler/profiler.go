package profiler

import (
	"github.com/hauke96/sigolo/v2"
	"runtime"
	"time"
)

/*
Add this to the beginning of a function to measure its performance:

key := profiler.StartMeasurement()
defer profiler.EndMeasurement(key)
*/

var Active = false
var ProfilingResults = map[string]int64{}
var CallingResults = map[string]int64{}

func StartMeasurement() int64 {
	if !Active {
		return 0
	}

	return time.Now().UnixNano()
}

func EndMeasurement(measurementStart int64) {
	if !Active {
		return
	}

	now := time.Now().UnixNano()

	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	name := f.Name()

	if _, ok := ProfilingResults[name]; !ok {
		ProfilingResults[name] = 0
		CallingResults[name] = 0
	}
	ProfilingResults[name] += now - measurementStart
	CallingResults[name] += 1
}

func PrintResults() {
	if !Active {
		return
	}

	sigolo.Infof("Profiling measurement results:")
	for k, v := range ProfilingResults {
		sigolo.Infof("  %s -> %d s (%d calls)", k, v/1_000_000_000.0, CallingResults[k])
	}
}
