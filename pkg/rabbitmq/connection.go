package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionState represents the current connection state.
type ConnectionState int32

const (
	ConnectionStateDisconnected ConnectionState = iota
	ConnectionStateConnecting
	ConnectionStateConnected
	ConnectionStateClosed
)

func (s ConnectionState) String() string {
	switch s {
	case ConnectionStateDisconnected:
		return "disconnected"
	case ConnectionStateConnecting:
		return "connecting"
	case ConnectionStateConnected:
		return "connected"
	case ConnectionStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ReconnectListener is called when a reconnection occurs.
type ReconnectListener func()

// ConnectionManager manages an AMQP connection with automatic reconnection
// across a cluster of RabbitMQ hosts.
type ConnectionManager struct {
	cfg    Config
	logger *slog.Logger

	conn      *amqp.Connection
	connMu    sync.RWMutex
	state     atomic.Int32
	closeOnce sync.Once

	notifyClose chan *amqp.Error

	reconnectListeners   []ReconnectListener
	reconnectListenersMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(cfg Config, logger *slog.Logger) *ConnectionManager {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cm := &ConnectionManager{
		cfg:    cfg,
		logger: logger.With("component", "connection_manager"),
		ctx:    ctx,
		cancel: cancel,
	}
	cm.state.Store(int32(ConnectionStateDisconnected))
	return cm
}

// Connect establishes the initial connection and starts the reconnect loop.
func (cm *ConnectionManager) Connect(ctx context.Context) error {
	if err := cm.connect(ctx); err != nil {
		return fmt.Errorf("rabbitmq: initial connection failed: %w", err)
	}
	cm.wg.Add(1)
	go cm.reconnectLoop()
	return nil
}

// Connection returns the current AMQP connection. May be nil if disconnected.
func (cm *ConnectionManager) Connection() *amqp.Connection {
	cm.connMu.RLock()
	defer cm.connMu.RUnlock()
	return cm.conn
}

// State returns the current connection state.
func (cm *ConnectionManager) State() ConnectionState {
	return ConnectionState(cm.state.Load())
}

// IsConnected returns whether the connection is currently active.
func (cm *ConnectionManager) IsConnected() bool {
	return cm.State() == ConnectionStateConnected
}

// OnReconnect registers a listener to be called after successful reconnection.
func (cm *ConnectionManager) OnReconnect(fn ReconnectListener) {
	cm.reconnectListenersMu.Lock()
	defer cm.reconnectListenersMu.Unlock()
	cm.reconnectListeners = append(cm.reconnectListeners, fn)
}

// Close gracefully closes the connection manager and the underlying connection.
func (cm *ConnectionManager) Close() error {
	var err error
	cm.closeOnce.Do(func() {
		cm.state.Store(int32(ConnectionStateClosed))
		cm.cancel()
		cm.wg.Wait()
		cm.connMu.Lock()
		defer cm.connMu.Unlock()
		if cm.conn != nil && !cm.conn.IsClosed() {
			err = cm.conn.Close()
		}
		cm.conn = nil
	})
	return err
}

// connect attempts to dial each host in shuffled order until one succeeds.
func (cm *ConnectionManager) connect(ctx context.Context) error {
	cm.state.Store(int32(ConnectionStateConnecting))

	hosts := cm.shuffledHosts()
	var lastErr error

	for _, host := range hosts {
		select {
		case <-ctx.Done():
			cm.state.Store(int32(ConnectionStateDisconnected))
			return ctx.Err()
		default:
		}

		uri := cm.buildURI(host)
		cm.logger.Info("connecting to RabbitMQ", "host", host)

		amqpCfg := amqp.Config{
			Heartbeat: cm.cfg.Heartbeat,
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, 10*time.Second)
			},
		}

		conn, err := amqp.DialConfig(uri, amqpCfg)
		if err != nil {
			lastErr = err
			cm.logger.Warn("connection attempt failed", "host", host, "error", err)
			continue
		}

		cm.connMu.Lock()
		cm.conn = conn
		cm.notifyClose = conn.NotifyClose(make(chan *amqp.Error, 1))
		cm.connMu.Unlock()

		cm.state.Store(int32(ConnectionStateConnected))
		cm.logger.Info("connected to RabbitMQ", "host", host)
		return nil
	}

	cm.state.Store(int32(ConnectionStateDisconnected))
	return fmt.Errorf("rabbitmq: failed to connect to any host: %w", lastErr)
}

func (cm *ConnectionManager) reconnectLoop() {
	defer cm.wg.Done()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case amqpErr, ok := <-cm.notifyClose:
			if !ok {
				if cm.State() == ConnectionStateClosed {
					return
				}
			}
			if amqpErr != nil {
				cm.logger.Error("connection lost", "error", amqpErr)
			} else {
				cm.logger.Warn("connection closed")
			}
			cm.state.Store(int32(ConnectionStateDisconnected))

			if cm.State() == ConnectionStateClosed {
				return
			}

			cm.attemptReconnect()
		}
	}
}

func (cm *ConnectionManager) attemptReconnect() {
	attempt := 0
	maxAttempts := cm.cfg.MaxReconnectAttempts

	for {
		if cm.State() == ConnectionStateClosed {
			return
		}

		select {
		case <-cm.ctx.Done():
			return
		default:
		}

		if maxAttempts > 0 && attempt >= maxAttempts {
			cm.logger.Error("max reconnect attempts reached, giving up",
				"attempts", attempt)
			return
		}

		delay := cm.cfg.ReconnectDelay(attempt)
		cm.logger.Info("attempting reconnect",
			"attempt", attempt+1,
			"delay", fmt.Sprintf("%dms", delay.Milliseconds()),
		)

		select {
		case <-cm.ctx.Done():
			return
		case <-time.After(delay):
		}

		if err := cm.connect(cm.ctx); err != nil {
			cm.logger.Error("reconnect failed", "attempt", attempt+1, "error", err)
			attempt++
			continue
		}

		cm.logger.Info("reconnected successfully", "attempt", attempt+1)
		cm.notifyReconnectListeners()
		return
	}
}

func (cm *ConnectionManager) notifyReconnectListeners() {
	cm.reconnectListenersMu.RLock()
	listeners := make([]ReconnectListener, len(cm.reconnectListeners))
	copy(listeners, cm.reconnectListeners)
	cm.reconnectListenersMu.RUnlock()

	for _, fn := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					cm.logger.Error("panic in reconnect listener", "panic", r)
				}
			}()
			fn()
		}()
	}
}

// shuffledHosts returns the configured hosts in random order.
func (cm *ConnectionManager) shuffledHosts() []string {
	hosts := make([]string, len(cm.cfg.Hosts))
	copy(hosts, cm.cfg.Hosts)
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
	return hosts
}

// buildURI constructs an AMQP URI from host and credentials.
func (cm *ConnectionManager) buildURI(host string) string {
	vhost := cm.cfg.VHost
	if vhost == "" {
		vhost = "/"
	}
	if vhost == "/" {
		return fmt.Sprintf("amqp://%s:%s@%s/", cm.cfg.Username, cm.cfg.Password, host)
	}
	return fmt.Sprintf("amqp://%s:%s@%s/%s", cm.cfg.Username, cm.cfg.Password, host, vhost)
}
