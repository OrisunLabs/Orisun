package config

import (
	"bytes"
	_ "embed"
	"fmt"
	logger "log"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// AppConfig represents the application configuration
type AppConfig struct {
	Backend      BackendConfig
	Postgres     PostgresDBConfig
	Sqlite       SqliteConfig
	FoundationDB FoundationDBConfig
	Grpc         struct {
		Port                         string
		EnableReflection             bool
		ConnectionTimeout            time.Duration
		KeepAliveTime                time.Duration
		KeepAliveTimeout             time.Duration
		MaxConcurrentStreams         uint32
		MaxReceiveMessageSize        int
		MaxSendMessageSize           int
		InitialWindowSize            int32
		InitialConnWindowSize        int32
		WriteBufferSize              int
		ReadBufferSize               int
		KeepaliveMinTime             time.Duration
		KeepalivePermitWithoutStream bool
		TLS                          struct {
			Enabled            bool
			CertFile           string
			KeyFile            string
			CAFile             string
			ClientAuthRequired bool
		}
	}

	PollingPublisher struct {
		BatchSize uint32
	}

	Logging LoggingConfig

	Nats NatsConfig
	// Prod bool
	Auth struct {
		AdminUsername string
		AdminPassword string
	}

	Admin AdminConfig

	Pprof struct {
		Enabled bool
		Port    string
	}

	OpenTelemetry struct {
		Enabled     bool
		Endpoint    string
		ServiceName string
	}
}

type LoggingConfig struct {
	Enabled bool
	Level   string // e.g., "debug", "info", "warn", "error"
}

// BackendConfig selects the storage driver. Values: "postgres" (default), "sqlite", or "foundationdb".
type BackendConfig struct {
	Type string
}

// FoundationDBConfig holds settings for the FoundationDB backend.
// The real backend is built with the "foundationdb" build tag because the Go
// binding requires native FoundationDB client libraries.
type FoundationDBConfig struct {
	ClusterFile string
	APIVersion  int
	Root        string
	// TransactionTimeoutMs bounds each FoundationDB transaction including its
	// internal retries. 0 = backend default (10s); negative = no timeout.
	TransactionTimeoutMs int
	// TransactionRetryLimit caps internal retries per transaction. 0 = unlimited
	// (the timeout is the bound).
	TransactionRetryLimit int
}

// SqliteConfig holds settings for the embedded SQLite backend.
// Each boundary gets its own event file at {Dir}/{boundary}.db and metadata
// file at {Dir}/{boundary}_metadata.db.
type SqliteConfig struct {
	Dir                string
	Synchronous        string
	BusyTimeoutMs      int
	ReadPoolSize       int
	CacheSize          int
	MmapSize           int64
	WalAutoCheckpoint  int
	TempStore          string
	PublisherWakeDelay time.Duration
	GroupCommit        SqliteGroupCommitConfig
}

// SqliteGroupCommitConfig tunes the per-boundary write batcher (the only
// SQLite write path). Zero values fall back to the package defaults in
// sqlite/group_commit.go; negative values are rejected at startup.
type SqliteGroupCommitConfig struct {
	// MaxBatchRequests caps requests per flush (savepoint-loop length).
	MaxBatchRequests int
	// MaxBatchEvents caps events per flush (transaction size). A request that
	// would exceed it is carried to the next flush.
	MaxBatchEvents int
	// MaxDelay > 0 makes the worker wait up to this long to fill a batch.
	// 0 (default) = opportunistic batching only: flush whatever is queued.
	MaxDelay time.Duration
	// MaxPending bounds the per-boundary queue; a full queue blocks callers.
	MaxPending int
	// FlushTimeout bounds one whole flush (worker-owned context).
	FlushTimeout time.Duration
}

// admin
type AdminConfig struct {
	Port     string
	Boundary string
}

// postgres
type PostgresDBConfig struct {
	User        string
	Name        string
	Password    string
	Host        string
	Port        string
	AdminSchema string
	SSLMode     string
	Dialect     string
	// Write pool configuration (optimized for write operations)
	WriteMaxOpenConns    int
	WriteMaxIdleConns    int
	WriteConnMaxIdleTime time.Duration
	WriteConnMaxLifetime time.Duration
	// Read pool configuration (optimized for read operations)
	ReadMaxOpenConns    int
	ReadMaxIdleConns    int
	ReadConnMaxIdleTime time.Duration
	ReadConnMaxLifetime time.Duration
	// AdminConfig pool configuration (optimized for admin operations)
	AdminMaxOpenConns    int
	AdminMaxIdleConns    int
	AdminConnMaxIdleTime time.Duration
	AdminConnMaxLifetime time.Duration
	ListenEnabled        bool
}

func (p PostgresDBConfig) DatabaseDialect() string {
	if p.Dialect == "" {
		return "postgres"
	}
	return strings.ToLower(strings.TrimSpace(p.Dialect))
}

type BoundaryToPostgresSchemaMapping struct {
	Schema   string
	Boundary string
}

func (p PostgresDBConfig) BootstrapSchemaMapping(adminBoundary string) map[string]BoundaryToPostgresSchemaMapping {
	return map[string]BoundaryToPostgresSchemaMapping{
		adminBoundary: {
			Boundary: adminBoundary,
			Schema:   p.AdminSchema,
		},
	}
}

// nats
type NatsConfig struct {
	URL                    string
	ServerName             string
	Port                   int
	MaxPayload             int32
	MaxConnections         int
	StoreDir               string
	EventStreamMaxBytes    int64
	EventStreamMaxMsgs     int64
	EventStreamMaxAge      time.Duration
	PublishAsyncMaxPending int
	Cluster                NatsClusterConfig
}

type NatsClusterConfig struct {
	Name     string
	Host     string
	Port     int
	Routes   string
	Username string
	Password string
	Enabled  bool
	Timeout  time.Duration
}

func (c *NatsClusterConfig) GetRoutes() []string {
	return strings.Split(c.Routes, ",")
}

//go:embed config.yaml
var configData []byte

func LoadConfig() (AppConfig, error) {
	viper.SetConfigType("yaml")

	if err := viper.ReadConfig(bytes.NewReader(configData)); err != nil {
		return AppConfig{}, fmt.Errorf("failed to read config data: %w", err)
	}

	// Correct environment variable substitution
	for _, key := range viper.AllKeys() {
		value := viper.Get(key)
		if s, ok := value.(string); ok {
			substituted := substituteEnvVars(s)
			viper.Set(key, substituted)
		}
	}

	var config AppConfig

	if err := viper.Unmarshal(&config); err != nil {
		return AppConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	err := validateConfig(config)
	if err != nil {
		return AppConfig{}, err
	}
	return config, nil
}

// BackendType returns the configured backend, defaulting to "postgres" if unset.
func (c *AppConfig) BackendType() string {
	if c.Backend.Type == "" {
		return "postgres"
	}
	return c.Backend.Type
}

func validateConfig(config AppConfig) error {
	switch config.Backend.Type {
	case "", "postgres":
		// ok
	case "sqlite":
		if config.Nats.Cluster.Enabled {
			return fmt.Errorf("sqlite backend does not support NATS clustering (single-node only); set ORISUN_NATS_CLUSTER_ENABLED=false")
		}
		if config.Sqlite.Dir == "" {
			return fmt.Errorf("sqlite backend requires ORISUN_SQLITE_DIR")
		}
		if config.Sqlite.PublisherWakeDelay < 0 {
			return fmt.Errorf("sqlite publisher wake delay must be >= 0, got %s", config.Sqlite.PublisherWakeDelay)
		}
	case "foundationdb":
		if config.FoundationDB.Root == "" {
			return fmt.Errorf("foundationdb backend requires ORISUN_FDB_ROOT")
		}
		if config.FoundationDB.APIVersion == 0 {
			return fmt.Errorf("foundationdb backend requires ORISUN_FDB_API_VERSION")
		}
	default:
		return fmt.Errorf("unknown backend type %q (expected 'postgres', 'sqlite', or 'foundationdb')", config.Backend.Type)
	}

	switch config.Postgres.DatabaseDialect() {
	case "postgres", "yugabyte":
		// ok
	default:
		return fmt.Errorf("unknown postgres dialect %q (expected 'postgres' or 'yugabyte')", config.Postgres.Dialect)
	}

	if err := validateBoundaryName(config.Admin.Boundary); err != nil {
		return fmt.Errorf("invalid admin boundary name %q: %w", config.Admin.Boundary, err)
	}
	if config.BackendType() == "postgres" {
		if err := validateBoundaryName(config.Postgres.AdminSchema); err != nil {
			return fmt.Errorf("invalid ORISUN_PG_ADMIN_SCHEMA %q: %w", config.Postgres.AdminSchema, err)
		}
	}
	return nil
}

// validateBoundaryName checks if a boundary name is a valid PostgreSQL identifier.
// PostgreSQL identifiers:
// - Must start with a letter (a-z, A-Z) or underscore (_)
// - Can contain letters, digits (0-9), and underscores
// - Maximum length is 63 characters
func validateBoundaryName(boundary string) error {
	if len(boundary) == 0 || len(boundary) > 63 {
		return fmt.Errorf("boundary name must be 1-63 characters, got %d", len(boundary))
	}

	firstChar := boundary[0]
	if !isLetter(firstChar) && firstChar != '_' {
		return fmt.Errorf("boundary name must start with a letter or underscore, got '%c'", firstChar)
	}

	for i := 1; i < len(boundary); i++ {
		c := boundary[i]
		if !isLetter(c) && !isDigit(c) && c != '_' {
			return fmt.Errorf("boundary name can only contain letters, digits, and underscores, invalid char '%c' at position %d", c, i)
		}
	}

	return nil
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func substituteEnvVars(value string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		// Extract content between ${ and }
		content := value[2 : len(value)-1]

		// Find the position of the first colon that's not within backticks
		var colonPos int = -1
		inBackticks := false
		for i, char := range content {
			if char == '`' {
				inBackticks = !inBackticks
			} else if char == ':' && !inBackticks {
				colonPos = i
				break
			}
		}

		content = strings.ReplaceAll(content, "`", "")
		envVar := content
		defaultValue := ""
		if colonPos != -1 {
			envVar = content[:colonPos]
			defaultValue = content[colonPos+1:]
		}

		if envValue := os.Getenv(envVar); envValue != "" {
			return envValue
		}
		return defaultValue
	}
	return value
}

func InitializeConfig() AppConfig {
	config, err := LoadConfig()
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}
	return config
}
