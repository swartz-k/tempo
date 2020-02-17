package distributor

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	cortex_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_util "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/limiter"

	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	opentelemetry_proto_collector_trace_v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/collector/traces/v1"
	opentelemetry_proto_trace_v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/frigg/pkg/distributor/receiver"
	"github.com/grafana/frigg/pkg/friggpb"
	"github.com/grafana/frigg/pkg/ingester/client"
	"github.com/grafana/frigg/pkg/util"
	"github.com/grafana/frigg/pkg/util/validation"
)

var (
	metricIngesterAppends = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "frigg",
		Name:      "distributor_ingester_appends_total",
		Help:      "The total number of batch appends sent to ingesters.",
	}, []string{"ingester"})
	metricIngesterAppendFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "frigg",
		Name:      "distributor_ingester_append_failures_total",
		Help:      "The total number of failed batch appends sent to ingesters.",
	}, []string{"ingester"})
	metricSpansIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "frigg",
		Name:      "distributor_spans_received_total",
		Help:      "The total number of spans received per tenant",
	}, []string{"tenant"})

	readinessProbeSuccess = []byte("Ready")
)

// Distributor coordinates replicates and distribution of log streams.
type Distributor struct {
	cfg           Config
	clientCfg     client.Config
	ingestersRing ring.ReadRing
	overrides     *validation.Overrides
	pool          *cortex_client.Pool

	// receiver shims
	receivers receiver.Receivers

	// The global rate limiter requires a distributors ring to count
	// the number of healthy instances.
	distributorsRing *ring.Lifecycler

	// Per-user rate limiter.
	ingestionRateLimiter *limiter.RateLimiter
}

// TODO taken from Loki taken from Cortex, see if we can refactor out an usable interface.
type pushTracker struct {
	samplesPending int32
	done           chan struct{}
	err            chan error
}

// New a distributor creates.
func New(cfg Config, clientCfg client.Config, ingestersRing ring.ReadRing, overrides *validation.Overrides, authEnabled bool) (*Distributor, error) {
	factory := cfg.factory
	if factory == nil {
		factory = func(addr string) (grpc_health_v1.HealthClient, error) {
			return client.New(clientCfg, addr)
		}
	}

	// Create the configured ingestion rate limit strategy (local or global).
	var ingestionRateStrategy limiter.RateLimiterStrategy
	var distributorsRing *ring.Lifecycler

	if overrides.IngestionRateStrategy() == validation.GlobalIngestionRateStrategy {
		var err error
		distributorsRing, err = ring.NewLifecycler(cfg.DistributorRing.ToLifecyclerConfig(), nil, "distributor", ring.DistributorRingKey)
		if err != nil {
			return nil, err
		}

		distributorsRing.Start()

		ingestionRateStrategy = newGlobalIngestionRateStrategy(overrides, distributorsRing)
	} else {
		ingestionRateStrategy = newLocalIngestionRateStrategy(overrides)
	}

	d := &Distributor{
		cfg:                  cfg,
		clientCfg:            clientCfg,
		ingestersRing:        ingestersRing,
		distributorsRing:     distributorsRing,
		overrides:            overrides,
		pool:                 cortex_client.NewPool(clientCfg.PoolConfig, ingestersRing, factory, cortex_util.Logger),
		ingestionRateLimiter: limiter.NewRateLimiter(ingestionRateStrategy, 10*time.Second),
	}

	if len(cfg.Receivers) > 0 {
		var err error
		d.receivers, err = receiver.New(cfg.Receivers, d, authEnabled)
		if err != nil {
			return nil, err
		}
		err = d.receivers.Start()
		if err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (d *Distributor) Stop() {
	if d.distributorsRing != nil {
		d.distributorsRing.Shutdown()
	}

	if d.receivers != nil {
		err := d.receivers.Shutdown()
		if err != nil {
			level.Error(cortex_util.Logger).Log("msg", "error stopping receivers", "error", err)
		}
	}
}

// ReadinessHandler is used to indicate to k8s when the distributor is ready.
// Returns 200 when the distributor is ready, 500 otherwise.
func (d *Distributor) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	_, err := d.ingestersRing.GetAll()
	if err != nil {
		http.Error(w, "Not ready: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(readinessProbeSuccess); err != nil {
		level.Error(cortex_util.Logger).Log("msg", "error writing success message", "error", err)
	}
}

// Push a set of streams.
func (d *Distributor) Push(ctx context.Context, resourceSpans *opentelemetry_proto_collector_trace_v1.ResourceSpans) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	spanCount := len(resourceSpans.Spans)
	if spanCount == 0 {
		return nil
	}
	metricSpansIngested.WithLabelValues(userID).Add(float64(spanCount))

	now := time.Now()
	if !d.ingestionRateLimiter.AllowN(now, userID, spanCount) {
		// Return a 4xx here to have the client discard the data and not retry. If a client
		// is sending too much data consistently we will unlikely ever catch up otherwise.
		validation.DiscardedSamples.WithLabelValues(validation.RateLimited, userID).Add(float64(spanCount))
		// todo: how does all this http/grpc stuff work with the shim
		return httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (%d bytes) exceeded while adding %d spans", int(d.ingestionRateLimiter.Limit(now, userID)), spanCount)
	}

	requestsByIngester, totalRequests, err := d.routeSpans(resourceSpans, userID, spanCount)
	if err != nil {
		return err
	}

	tracker := pushTracker{
		samplesPending: int32(totalRequests),
		done:           make(chan struct{}),
		err:            make(chan error),
	}
	for ingester, reqs := range requestsByIngester {
		go func(ingesterAddr string, reqs []*opentelemetry_proto_collector_trace_v1.ResourceSpans, tracker *pushTracker) {
			// Use a background context to make sure all ingesters get samples even if we return early
			localCtx, cancel := context.WithTimeout(context.Background(), d.clientCfg.RemoteTimeout)
			defer cancel()
			localCtx = user.InjectOrgID(localCtx, userID)
			if sp := opentracing.SpanFromContext(ctx); sp != nil {
				localCtx = opentracing.ContextWithSpan(localCtx, sp)
			}
			d.sendSamples(localCtx, ingesterAddr, reqs, tracker)
		}(ingester, reqs, &tracker)
	}

	select {
	case err := <-tracker.err:
		return err
	case <-tracker.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TODO taken from Loki taken from Cortex, see if we can refactor out an usable interface.
func (d *Distributor) sendSamples(ctx context.Context, ingesterAddr string, batches []*opentelemetry_proto_collector_trace_v1.ResourceSpans, pushTracker *pushTracker) {

	for _, b := range batches {
		err := d.sendSamplesErr(ctx, ingesterAddr, b)

		if err != nil {
			pushTracker.err <- err
		} else {
			if atomic.AddInt32(&pushTracker.samplesPending, -1) == 0 {
				pushTracker.done <- struct{}{}
			}
		}
	}
}

// TODO taken from Loki taken from Cortex, see if we can refactor out an usable interface.
func (d *Distributor) sendSamplesErr(ctx context.Context, ingesterAddr string, req *opentelemetry_proto_collector_trace_v1.ResourceSpans) error {
	c, err := d.pool.GetClientFor(ingesterAddr)
	if err != nil {
		return err
	}

	if len(req.Spans) == 0 {
		return nil
	}

	id := req.Spans[0].TraceId
	protoBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	_, err = c.(friggpb.PusherClient).Push(ctx, &friggpb.PushRequest{
		Id:            id,
		ResourceSpans: protoBytes,
	})
	metricIngesterAppends.WithLabelValues(ingesterAddr).Inc()
	if err != nil {
		metricIngesterAppendFailures.WithLabelValues(ingesterAddr).Inc()
	}
	return err
}

// Check implements the grpc healthcheck
func (*Distributor) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (d *Distributor) routeSpans(resourceSpans *opentelemetry_proto_collector_trace_v1.ResourceSpans, userID string, spanCount int) (map[string][]*opentelemetry_proto_collector_trace_v1.ResourceSpans, int, error) {
	const maxExpectedReplicationSet = 3 // 3.  b/c frigg it
	var descs [maxExpectedReplicationSet]ring.IngesterDesc

	// todo: add a metric to track traces per batch
	requests := make(map[uint32]*opentelemetry_proto_collector_trace_v1.ResourceSpans)
	for _, span := range resourceSpans.Spans {
		if !validation.ValidTraceID(span.TraceId) {
			return nil, 0, httpgrpc.Errorf(http.StatusBadRequest, "trace ids must be 128 bit")
		}

		key := util.TokenFor(userID, span.TraceId)

		routedSpans, ok := requests[key]
		if !ok {
			routedSpans = &opentelemetry_proto_collector_trace_v1.ResourceSpans{
				Spans:    make([]*opentelemetry_proto_trace_v1.Span, 0, spanCount), // assume most spans belong to the same trace
				Resource: resourceSpans.Resource,
			}
			requests[key] = routedSpans
		}
		routedSpans.Spans = append(routedSpans.Spans, span)
	}

	requestsByIngester := make(map[string][]*opentelemetry_proto_collector_trace_v1.ResourceSpans)
	for key, routedSpans := range requests {
		// now map to ingesters
		replicationSet, err := d.ingestersRing.Get(key, ring.Write, descs[:0])
		if err != nil {
			return nil, 0, err
		}
		for _, ingester := range replicationSet.Ingesters {
			requestsByIngester[ingester.Addr] = append(requestsByIngester[ingester.Addr], routedSpans)
		}
	}

	return requestsByIngester, len(requests), nil
}
