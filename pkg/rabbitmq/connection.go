package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection manages a single AMQP connection with automatic reconnection.
// Supports multiple cluster nodes with shuffle-based failover.
type Connection struct {
	cfg    ConnectionConfig
	logger *slog.Logger

	mu   sync.RWMutex
	conn *amqp.Connection

	connected atomic.Bool
	closed    atomic.Bool
	done      chan struct{}

	// Listeners notified on reconnect so channels can be re-established.
	listenersMu sync.Mutex
	listeners   map[int]chan struct{}
	listenerSeq int
}

// NewConnection creates a new managed AMQP connection.
func NewConnection(cfg ConnectionConfig, logger *slog.Logger) *Connection {
	return &Connection{
		cfg:       cfg,
		logger:    logger.With("component", "rabbitmq.connection"),
		done:      make(chan struct{}),
		listeners: make(map[int]chan struct{}),
	}
}

// Connect establishes the initial connection and starts the reconnect loop.
func (c *Connection) Connect(ctx context.Context) error {
	if err := c.dial(); err != nil {
		return fmt.Errorf("rabbitmq initial connection: %w", err)
	}
	go c.reconnectLoop()
	return nil
}

// RawConnection returns the underlying amqp.Connection.
func (c *Connection) RawConnection() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// HealthCheck verifies the connection is alive and usable.
func (c *Connection) HealthCheck() error {
	if c.closed.Load() {
		return fmt.Errorf("connection is closed")
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil || conn.IsClosed() {
		return ErrNotConnected
	}
	return nil
}

// NotifyReconnect returns a channel that receives a signal every time
// a new connection is established after a disconnection, and an
// unsubscribe function to clean up the listener.
func (c *Connection) NotifyReconnect() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.listenersMu.Lock()
	c.listenerSeq++
	id := c.listenerSeq
	c.listeners[id] = ch
	c.listenersMu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			c.listenersMu.Lock()
			if _, ok := c.listeners[id]; ok {
				close(ch)
				delete(c.listeners, id)
			}
			c.listenersMu.Unlock()
		})
	}
	return ch, unsubscribe
}

// Close gracefully shuts down the connection and cleans up listener channels.
func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.connected.Store(false)
	close(c.done)

	c.listenersMu.Lock()
	for id, ch := range c.listeners {
		delete(c.listeners, id)
		close(ch)
	}
	c.listenersMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}
	return nil
}

func (c *Connection) dial() error {
	addresses := c.cfg.Addresses
	if len(addresses) == 0 {
		return fmt.Errorf("amqp dial: no addresses configured")
	}

	// Shuffle addresses to spread clients across cluster nodes
	// and avoid thundering herd on a single node.
	shuffled := make([]string, len(addresses))
	copy(shuffled, addresses)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	var lastErr error
	for _, address := range shuffled {
		host, portStr, err := net.SplitHostPort(address)
		if err != nil {
			lastErr = err
			c.logger.Warn("Invalid address format", "address", address, "error", err)
			continue
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			lastErr = err
			c.logger.Warn("Invalid port format", "address", address, "error", err)
			continue
		}

		uri := amqp.URI{
			Scheme:   "amqp",
			Host:     host,
			Port:     port,
			Username: c.cfg.Username,
			Password: c.cfg.Password,
			Vhost:    c.cfg.VirtualHost,
		}

		conn, err := amqp.DialConfig(uri.String(), amqp.Config{
			Dial:      amqp.DefaultDial(c.cfg.DialTimeout),
			Heartbeat: c.cfg.RequestedHeartbeat,
			Locale:    "en_US",
		})
		if err != nil {
			lastErr = err
			c.logger.Warn("Failed to connect to node", "host", host, "error", err)
			continue
		}

		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()
		c.connected.Store(true)

		return nil
	}

	return fmt.Errorf("amqp dial: all nodes failed: %w", lastErr)
}

// reconnectLoop watches for connection loss and drives reconnection.
func (c *Connection) reconnectLoop() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in reconnect loop, restarting", "panic", r)
			if !c.closed.Load() {
				go c.reconnectLoop()
			}
		}
	}()

	for {
		if c.closed.Load() {
			return
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			c.handleReconnect()
			continue
		}

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case amqpErr, ok := <-notifyClose:
			if c.closed.Load() {
				return
			}
			c.connected.Store(false)
			if ok {
				c.logger.Error("Connection lost", "error", amqpErr)
			} else {
				c.logger.Warn("Connection closed, reconnecting")
			}
			c.handleReconnect()
		case <-c.done:
			return
		}
	}
}

func (c *Connection) handleReconnect() {
	// Close old connection to prevent leak.
	c.mu.Lock()
	old := c.conn
	c.conn = nil
	c.mu.Unlock()
	if old != nil && !old.IsClosed() {
		old.Close()
	}

	delay := c.cfg.ReconnectDelay

	for attempt := 1; ; attempt++ {
		// Add jitter: ±25% of delay to prevent thundering herd.
		jitter := time.Duration(float64(delay) * (0.75 + rand.Float64()*0.5))

		// Interruptible sleep — cancelled immediately by Close().
		if !c.sleep(jitter) {
			return
		}

		if err := c.dial(); err != nil {
			c.logger.Error("Reconnect failed", "attempt", attempt, "error", err)
			delay = min(delay*2, c.cfg.MaxReconnectDelay)
			continue
		}

		c.notifyListeners()
		return
	}
}

func (c *Connection) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-c.done:
		return false
	}
}

func (c *Connection) notifyListeners() {
	c.listenersMu.Lock()
	defer c.listenersMu.Unlock()
	for _, ch := range c.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
