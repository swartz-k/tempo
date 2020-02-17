package util

import (
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/frigg/pkg/friggpb"
	opentelemetry_proto_collector_trace_v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/collector/traces/v1"
)

func PushTraceToTrace(push *friggpb.PushTrace) (*friggpb.Trace, error) {
	trace := &friggpb.Trace{
		Batches: make([]*opentelemetry_proto_collector_trace_v1.ResourceSpans, 0, len(push.Batches)),
	}

	for _, b := range push.Batches {
		batch := &opentelemetry_proto_collector_trace_v1.ResourceSpans{}
		err := proto.Unmarshal(b, batch)
		if err != nil {
			return nil, err
		}

		trace.Batches = append(trace.Batches, batch)
	}

	return trace, nil
}
