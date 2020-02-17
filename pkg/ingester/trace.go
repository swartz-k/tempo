package ingester

import (
	"context"
	"time"

	"github.com/grafana/frigg/pkg/friggpb"
)

type trace struct {
	trace      *friggpb.PushTrace
	fp         traceFingerprint
	lastAppend time.Time
	traceID    []byte
}

func newTrace(fp traceFingerprint, traceID []byte) *trace {
	return &trace{
		fp: fp,
		trace: &friggpb.PushTrace{
			Batches: make([][]byte, 0, 10), // todo: 10 to reduce allocations.  total guess.  could use metrics
		},
		lastAppend: time.Now(),
		traceID:    traceID,
	}
}

func (t *trace) Push(_ context.Context, req *friggpb.PushRequest) error {
	t.trace.Batches = append(t.trace.Batches, req.ResourceSpans)
	t.lastAppend = time.Now()

	return nil
}
