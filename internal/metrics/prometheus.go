package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	S3RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nos3_requests_total",
		Help: "Total S3 API requests",
	}, []string{"method", "operation", "status"})

	S3RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nos3_request_duration_seconds",
		Help:    "S3 API request latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30},
	}, []string{"operation"})

	S3BytesIn = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nos3_bytes_received_total",
		Help: "Total bytes received via PutObject/UploadPart",
	}, []string{"bucket"})

	S3BytesOut = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nos3_bytes_sent_total",
		Help: "Total bytes sent via GetObject",
	}, []string{"bucket"})

	S3Errors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nos3_errors_total",
		Help: "S3 API errors by error code",
	}, []string{"code"})
)

func RunServer(ctx context.Context, cfg config.MetricsConfig) error {
	mux := http.NewServeMux()
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}
	mux.Handle(path, promhttp.Handler())

	srv := &http.Server{Addr: cfg.Listen, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
