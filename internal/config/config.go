package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NATS          NATSConfig          `yaml:"nats"`
	S3            S3Config            `yaml:"s3"`
	Auth          AuthConfig          `yaml:"auth"`
	Buckets       BucketsConfig       `yaml:"buckets"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type NATSConfig struct {
	URL            string    `yaml:"url"`
	CredentialsFile string   `yaml:"credentials_file"`
	NKeySeedFile   string    `yaml:"nkey_seed_file"`
	TLS            TLSConfig `yaml:"tls"`
	ConnectionName string    `yaml:"connection_name"`
	MaxReconnects  int       `yaml:"max_reconnects"`
	ReconnectWait  Duration  `yaml:"reconnect_wait"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type S3Config struct {
	Listen     string `yaml:"listen"`
	Region     string `yaml:"region"`
	HostStyle  string `yaml:"host_style"`
	BaseDomain string `yaml:"base_domain"`
}

type AuthConfig struct {
	Enabled        bool   `yaml:"enabled"`
	AccessKeyID    string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
}

type BucketsConfig struct {
	AutoCreate    bool     `yaml:"auto_create"`
	AllowCreate   bool     `yaml:"allow_create"`
	AllowDelete   bool     `yaml:"allow_delete"`
	AllowList     []string `yaml:"allow_list"`
	DefaultChunk  int      `yaml:"default_chunk"`
	MaxObjectSize int64    `yaml:"max_object_size"`
}

type ObservabilityConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
	Logging LoggingConfig `yaml:"logging"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
	Path    string `yaml:"path"`
}

type HealthConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Listen        string `yaml:"listen"`
	LivenessPath  string `yaml:"liveness_path"`
	ReadinessPath string `yaml:"readiness_path"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if c.NATS.URL == "" {
		return fmt.Errorf("nats.url is required")
	}
	if c.S3.Listen == "" {
		return fmt.Errorf("s3.listen is required")
	}
	return nil
}

func DefaultConfig() *Config {
	return &Config{
		NATS: NATSConfig{
			URL:            "nats://nats.wasmcloud-system:4222",
			ConnectionName: "nats-objstore-s3",
			MaxReconnects:  -1,
			ReconnectWait:  Duration(2 * time.Second),
		},
		S3: S3Config{
			Listen:    ":9000",
			Region:    "us-east-1",
			HostStyle: "path",
		},
		Auth: AuthConfig{
			Enabled: false,
		},
		Buckets: BucketsConfig{
			AutoCreate:   true,
			AllowCreate:  true,
			AllowDelete:  true,
			DefaultChunk: 131072, // 128KB
		},
		Observability: ObservabilityConfig{
			Metrics: MetricsConfig{Enabled: true, Listen: ":9090", Path: "/metrics"},
			Health:  HealthConfig{Enabled: true, Listen: ":8081", LivenessPath: "/healthz", ReadinessPath: "/readyz"},
			Logging: LoggingConfig{Level: "info", Format: "json"},
		},
	}
}

// Duration wraps time.Duration for YAML unmarshaling.
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}
