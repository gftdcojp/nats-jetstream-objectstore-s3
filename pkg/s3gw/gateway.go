// Package s3gw provides an embeddable S3-compatible HTTP handler backed by NATS ObjectStore.
package s3gw

import (
	"net/http"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/auth"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/backend"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/s3api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// GatewayConfig configures the embeddable S3 gateway.
type GatewayConfig struct {
	NC              *nats.Conn
	JS              jetstream.JetStream
	AccessKeyID     string
	SecretAccessKey  string
	Region          string
	AutoCreateBucket bool
	DefaultChunk    int
	Logger          *zap.Logger
}

// Gateway provides an S3-compatible HTTP handler backed by NATS ObjectStore.
type Gateway struct {
	handler http.Handler
}

// New creates a new S3 gateway.
func New(cfg GatewayConfig) (*Gateway, error) {
	be := backend.NewNATSBackend(cfg.NC, cfg.JS, cfg.DefaultChunk, cfg.AutoCreateBucket, cfg.Logger.Named("backend"))

	var verifier *auth.SigV4Verifier
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		verifier = auth.NewSigV4Verifier(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Region)
	}

	router := s3api.NewRouter(s3api.RouterConfig{
		Backend: be,
		Auth:    verifier,
		Logger:  cfg.Logger.Named("s3api"),
		Region:  cfg.Region,
	})

	return &Gateway{handler: router}, nil
}

// Handler returns the http.Handler for the S3 API.
func (g *Gateway) Handler() http.Handler {
	return g.handler
}
