package nats

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	c "github.com/OrisunLabs/Orisun/config"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func InitializeNATS(ctx context.Context, config c.NatsConfig, logger l.Logger) (jetstream.JetStream, *natsgo.Conn, *server.Server) {
	runtime, err := Start(ctx, config, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize NATS: %v", err)
	}
	return runtime.JetStream, runtime.Conn, runtime.Server
}

type Runtime struct {
	JetStream jetstream.JetStream
	Conn      *natsgo.Conn
	Server    *server.Server

	ownsConn bool
}

// HealthCheck verifies that the JetStream control plane can answer a request.
// Connection status alone is insufficient because a NATS connection may be
// established while JetStream is unavailable.
func (r *Runtime) HealthCheck(ctx context.Context) error {
	if r == nil || r.JetStream == nil {
		return fmt.Errorf("JetStream runtime is not configured")
	}
	if r.Conn != nil && r.Conn.IsClosed() {
		return fmt.Errorf("NATS connection is closed")
	}
	if _, err := r.JetStream.AccountInfo(ctx); err != nil {
		return fmt.Errorf("query JetStream account info: %w", err)
	}
	return nil
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	if r.Conn != nil && r.ownsConn {
		r.Conn.Close()
	}
	if r.Server != nil {
		r.Server.Shutdown()
	}
}

type Option func(*options)

type options struct {
	url         string
	conn        *natsgo.Conn
	jetStream   jetstream.JetStream
	connectOpts []natsgo.Option
	jsOpts      []jetstream.JetStreamOpt
}

func WithURL(url string, opts ...natsgo.Option) Option {
	return func(o *options) {
		o.url = url
		o.connectOpts = append(o.connectOpts, opts...)
	}
}

func WithConnection(conn *natsgo.Conn, opts ...jetstream.JetStreamOpt) Option {
	return func(o *options) {
		o.conn = conn
		o.jsOpts = append(o.jsOpts, opts...)
	}
}

func WithJetStream(js jetstream.JetStream) Option {
	return func(o *options) {
		o.jetStream = js
	}
}

func WithConnectOptions(opts ...natsgo.Option) Option {
	return func(o *options) {
		o.connectOpts = append(o.connectOpts, opts...)
	}
}

func WithJetStreamOptions(opts ...jetstream.JetStreamOpt) Option {
	return func(o *options) {
		o.jsOpts = append(o.jsOpts, opts...)
	}
}

func Start(ctx context.Context, config c.NatsConfig, logger l.Logger, runtimeOpts ...Option) (*Runtime, error) {
	opts := options{url: config.URL}
	for _, apply := range runtimeOpts {
		apply(&opts)
	}

	if config.PublishAsyncMaxPending > 0 {
		opts.jsOpts = append(opts.jsOpts, jetstream.WithPublishAsyncMaxPending(config.PublishAsyncMaxPending))
	}

	if opts.jetStream != nil {
		return &Runtime{JetStream: opts.jetStream}, nil
	}

	if opts.conn != nil {
		js, err := jetstream.New(opts.conn, opts.jsOpts...)
		if err != nil {
			return nil, err
		}
		if err := waitForJetStream(ctx, js, logger); err != nil {
			return nil, err
		}
		return &Runtime{JetStream: js, Conn: opts.conn}, nil
	}

	if strings.TrimSpace(opts.url) != "" {
		nc, err := natsgo.Connect(opts.url, opts.connectOpts...)
		if err != nil {
			return nil, err
		}
		js, err := jetstream.New(nc, opts.jsOpts...)
		if err != nil {
			nc.Close()
			return nil, err
		}
		if err := waitForJetStream(ctx, js, logger); err != nil {
			nc.Close()
			return nil, err
		}
		logger.Info("Connected to external NATS at ", opts.url)
		return &Runtime{JetStream: js, Conn: nc, ownsConn: true}, nil
	}

	natsOptions := createNATSOptions(config, logger)
	natsServer, err := startNATSServer(natsOptions, natsStartupTimeout(config.Cluster.Timeout), logger)
	if err != nil {
		return nil, err
	}

	nc, err := natsgo.Connect("", natsgo.InProcessServer(natsServer))
	if err != nil {
		natsServer.Shutdown()
		return nil, err
	}

	js, err := jetstream.New(nc, opts.jsOpts...)
	if err != nil {
		nc.Close()
		natsServer.Shutdown()
		return nil, err
	}
	if err := waitForJetStream(ctx, js, logger); err != nil {
		nc.Close()
		natsServer.Shutdown()
		return nil, err
	}
	return &Runtime{JetStream: js, Conn: nc, Server: natsServer, ownsConn: true}, nil
}

func createNATSOptions(config c.NatsConfig, logger l.Logger) server.Options {
	options := server.Options{
		ServerName: config.ServerName,
		Port:       config.Port,
		MaxPayload: config.MaxPayload,
		JetStream:  true,
		StoreDir:   config.StoreDir,
	}

	if config.Cluster.Enabled {
		options.Cluster = server.ClusterOpts{
			Name: config.Cluster.Name,
			Host: config.Cluster.Host,
			Port: config.Cluster.Port,
		}
		options.Routes = convertToURLSlice(config.Cluster.GetRoutes(), logger)
		logger.Info("Nats cluster is enabled, running in clustered mode")
		logger.Info(
			"Cluster configuration: Name=%v, Host=%v, Port=%v, Routes=%v",
			config.Cluster.Name,
			config.Cluster.Host,
			config.Cluster.Port,
			config.Cluster.Routes,
		)
	} else {
		logger.Info("Nats cluster is disabled, running in standalone mode")
	}

	return options
}

func startNATSServer(options server.Options, timeout time.Duration, logger l.Logger) (*server.Server, error) {
	natsServer, err := server.NewServer(&options)
	if err != nil {
		return nil, err
	}

	natsServer.ConfigureLogger()
	go natsServer.Start()
	if !natsServer.ReadyForConnections(timeout) {
		natsServer.Shutdown()
		return nil, fmt.Errorf("NATS server failed to start")
	}
	logger.Info("NATS server started on ", natsServer.ClientURL())

	return natsServer, nil
}

func natsStartupTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

func waitForJetStream(ctx context.Context, js jetstream.JetStream, logger l.Logger) error {
	jetStreamTestDone := make(chan error, 1)

	go func() {
		for {
			if err := ctx.Err(); err != nil {
				jetStreamTestDone <- err
				return
			}

			streamName := "test_jetstream"
			_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name: streamName,
				Subjects: []string{
					streamName + ".test",
				},
				MaxMsgs: 1,
				Storage: jetstream.MemoryStorage,
			})

			if err != nil {
				logger.Warnf("failed to add stream: %v %v", streamName, err)
				select {
				case <-ctx.Done():
					jetStreamTestDone <- ctx.Err()
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}

			r, err := js.Publish(ctx, streamName+".test", []byte("test"))
			if err != nil {
				logger.Warnf("Failed to publish to JetStream, retrying in 5 second: %v", err)
				select {
				case <-ctx.Done():
					jetStreamTestDone <- ctx.Err()
					return
				case <-time.After(5 * time.Second):
				}
			} else {
				logger.Infof("Published to JetStream: %v", r)
				jetStreamTestDone <- nil
				break
			}
		}
	}()

	select {
	case err := <-jetStreamTestDone:
		if err != nil {
			return err
		}
		logger.Info("JetStream system is available")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func convertToURLSlice(routes []string, logger l.Logger) []*url.URL {
	var urls []*url.URL
	for _, route := range routes {
		u, err := url.Parse(route)
		if err != nil {
			logger.Fatalf("Warning: invalid route URL %q: %v", route, err)
			continue
		}
		urls = append(urls, u)
	}
	return urls
}
