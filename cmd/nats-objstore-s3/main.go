package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/auth"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/backend"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/config"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/metrics"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/s3api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("nats-objstore-s3 %s\n", version)
		os.Exit(0)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := newLogger(cfg.Observability.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if err := run(cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("fatal error", zap.Error(err))
	}
}

func run(cfg *config.Config, logger *zap.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to NATS
	nc, err := connectNATS(cfg.NATS, logger.Named("nats"))
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream context: %w", err)
	}

	// Initialize backend
	be := backend.NewNATSBackend(nc, js, cfg.Buckets.DefaultChunk, cfg.Buckets.AutoCreate, logger.Named("backend"))

	// Initialize auth
	var verifier *auth.SigV4Verifier
	if cfg.Auth.Enabled {
		verifier = auth.NewSigV4Verifier(cfg.Auth.AccessKeyID, cfg.Auth.SecretAccessKey, cfg.S3.Region)
	}

	// Build S3 router
	router := s3api.NewRouter(s3api.RouterConfig{
		Backend: be,
		Auth:    verifier,
		Logger:  logger.Named("s3api"),
		Region:  cfg.S3.Region,
	})

	g, gctx := errgroup.WithContext(ctx)

	// S3 API HTTP server
	g.Go(func() error {
		srv := &http.Server{Addr: cfg.S3.Listen, Handler: router}
		go func() {
			<-gctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			srv.Shutdown(shutdownCtx)
		}()
		logger.Info("S3 API server listening", zap.String("addr", cfg.S3.Listen))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Health server
	if cfg.Observability.Health.Enabled {
		checker := metrics.NewHealthChecker(nc)
		g.Go(func() error { return metrics.RunHealthServer(gctx, cfg.Observability.Health, checker) })
	}

	// Metrics server
	if cfg.Observability.Metrics.Enabled {
		g.Go(func() error { return metrics.RunServer(gctx, cfg.Observability.Metrics) })
	}

	logger.Info("nats-objstore-s3 started",
		zap.String("version", version),
		zap.String("nats_url", cfg.NATS.URL),
		zap.String("s3_listen", cfg.S3.Listen),
	)

	return g.Wait()
}

func connectNATS(cfg config.NATSConfig, logger *zap.Logger) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(cfg.ConnectionName),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait.Duration()),
		nats.ReconnectBufSize(16 * 1024 * 1024),
		nats.PingInterval(20 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warn("NATS disconnected", zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		opts = append(opts, nats.ClientCert(cfg.TLS.CertFile, cfg.TLS.KeyFile))
	}
	if cfg.TLS.CAFile != "" {
		opts = append(opts, nats.RootCAs(cfg.TLS.CAFile))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, err
	}
	logger.Info("connected to NATS", zap.String("url", nc.ConnectedUrl()))
	return nc, nil
}

func newLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	var zapCfg zap.Config
	if cfg.Format == "console" {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}
	switch cfg.Level {
	case "debug":
		zapCfg.Level.SetLevel(zap.DebugLevel)
	case "info":
		zapCfg.Level.SetLevel(zap.InfoLevel)
	case "warn":
		zapCfg.Level.SetLevel(zap.WarnLevel)
	case "error":
		zapCfg.Level.SetLevel(zap.ErrorLevel)
	}
	return zapCfg.Build()
}
