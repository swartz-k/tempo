package test

import (
	"math/rand"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/frigg/pkg/friggpb"
	opentelemetry_proto_collector_trace_v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/collector/traces/v1"
	opentelemetry_proto_trace_v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
)

func MakeRequest(spans int, traceID []byte) *friggpb.PushRequest {
	if len(traceID) == 0 {
		traceID = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	}

	sampleSpan := opentelemetry_proto_trace_v1.Span{
		Name:    "test",
		TraceId: traceID,
		SpanId:  []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	}

	resourceSpans := &opentelemetry_proto_collector_trace_v1.ResourceSpans{}

	for i := 0; i < spans; i++ {
		resourceSpans.Spans = append(resourceSpans.Spans, &sampleSpan)
	}

	bytes, _ := proto.Marshal(resourceSpans)
	return &friggpb.PushRequest{
		Id:            traceID,
		ResourceSpans: bytes,
	}
}

func MakeTrace(requests int, traceID []byte) *friggpb.PushTrace {
	trace := &friggpb.PushTrace{
		Batches: make([][]byte, 0),
	}

	for i := 0; i < requests; i++ {
		trace.Batches = append(trace.Batches, MakeRequest(rand.Int()%20+1, traceID).ResourceSpans)
	}

	return trace
}
