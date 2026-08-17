package mq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"notification-delivery/internal/config"
)

const rabbitAttemptsHeader = "x-notification-delivery-attempts"

// --- publisher -------------------------------------------------

// rabbitMQPublisher keeps a single long-lived connection/channel and
// republishes are left to the caller (the compensator already retries on
// error, per DESIGN.md §8's "at least once" publish semantics).
type rabbitMQPublisher struct {
	cfg  config.RabbitMQConfig
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newRabbitMQPublisher(cfg config.RabbitMQConfig) (*rabbitMQPublisher, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq confirm mode: %w", err)
	}
	if err := declareTopology(ch, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	return &rabbitMQPublisher{cfg: cfg, conn: conn, ch: ch}, nil
}

func declareTopology(ch *amqp.Channel, cfg config.RabbitMQConfig) error {
	if cfg.Exchange != "" {
		exchangeType := cfg.ExchangeType
		if exchangeType == "" {
			exchangeType = "direct"
		}
		if err := ch.ExchangeDeclare(cfg.Exchange, exchangeType, cfg.Durable, false, false, false, nil); err != nil {
			return fmt.Errorf("rabbitmq exchange declare: %w", err)
		}
	}
	if _, err := ch.QueueDeclare(cfg.Queue, cfg.Durable, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq queue declare: %w", err)
	}
	if cfg.Exchange != "" {
		if err := ch.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq queue bind: %w", err)
		}
	}
	return nil
}

func (p *rabbitMQPublisher) routingKey() string {
	if p.cfg.Exchange == "" {
		return p.cfg.Queue
	}
	return p.cfg.RoutingKey
}

func (p *rabbitMQPublisher) exchange() string {
	return p.cfg.Exchange
}

func (p *rabbitMQPublisher) Publish(ctx context.Context, eventID uuid.UUID) (PublishResult, error) {
	body, err := encode(eventID)
	if err != nil {
		return PublishResult{}, err
	}
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx,
		p.exchange(), p.routingKey(), true, false,
		amqp.Publishing{
			Headers:      amqp.Table{rabbitAttemptsHeader: int64(1)},
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    eventID.String(),
			Body:         body,
		})
	if err != nil {
		return PublishResult{}, fmt.Errorf("rabbitmq publish: %w", err)
	}
	ok, err := confirmation.WaitContext(ctx)
	if err != nil {
		return PublishResult{}, fmt.Errorf("rabbitmq publish confirm: %w", err)
	}
	if !ok {
		return PublishResult{}, fmt.Errorf("rabbitmq publish: broker nacked delivery")
	}
	return PublishResult{Durable: true}, nil
}

func (p *rabbitMQPublisher) Close() error {
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// --- consumer -------------------------------------------------

// rabbitMQConsumer reconnects on connection/channel loss so a broker
// restart does not require a worker restart. Multiple consumer processes
// bound to the same queue are competing consumers, which is how workers
// scale horizontally.
type rabbitMQConsumer struct {
	cfg         config.RabbitMQConfig
	conn        *amqp.Connection
	ch          *amqp.Channel
	retryCh     *amqp.Channel
	retryMu     sync.Mutex
	closed      chan struct{}
	concurrency int
	sem         chan struct{}
}

func newRabbitMQConsumer(cfg config.RabbitMQConfig) (*rabbitMQConsumer, error) {
	if err := validateRequeueConfig("rabbitmq", cfg.RequeueConfig); err != nil {
		return nil, err
	}
	return &rabbitMQConsumer{cfg: cfg, closed: make(chan struct{})}, nil
}

func (c *rabbitMQConsumer) connect() error {
	conn, err := amqp.Dial(c.cfg.URL)
	if err != nil {
		return fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := declareTopology(ch, c.cfg); err != nil {
		ch.Close()
		conn.Close()
		return err
	}
	retryCh, err := conn.Channel()
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq retry channel: %w", err)
	}
	if err := retryCh.Confirm(false); err != nil {
		retryCh.Close()
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq retry confirm mode: %w", err)
	}
	prefetch := c.cfg.PrefetchCount
	if prefetch < c.concurrency {
		prefetch = c.concurrency
	}
	if prefetch <= 0 {
		prefetch = 1
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		retryCh.Close()
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq qos: %w", err)
	}
	c.retryMu.Lock()
	c.conn, c.ch = conn, ch
	c.retryCh = retryCh
	c.retryMu.Unlock()
	return nil
}

func (c *rabbitMQConsumer) Start(ctx context.Context, concurrency int, handler Handler) error {
	if concurrency < 1 {
		concurrency = 1
	}
	c.concurrency = concurrency
	c.sem = make(chan struct{}, concurrency)

	reconnectDelay := c.cfg.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = 5 * time.Second
	}
	if err := c.connect(); err != nil {
		return err
	}
	go c.runLoop(ctx, handler, reconnectDelay)
	return nil
}

func (c *rabbitMQConsumer) runLoop(ctx context.Context, handler Handler, reconnectDelay time.Duration) {
	defer close(c.closed)
	for ctx.Err() == nil {
		deliveries, err := c.ch.Consume(c.cfg.Queue, "", false, false, false, false, nil)
		if err != nil {
			slog.Error("rabbitmq consume failed, reconnecting", "error", err)
			if !c.waitAndReconnect(ctx, reconnectDelay) {
				return
			}
			continue
		}
		connClosed := c.conn.NotifyClose(make(chan *amqp.Error, 1))
		if !c.consumeUntilClosed(ctx, deliveries, connClosed, handler) {
			return
		}
		if !c.waitAndReconnect(ctx, reconnectDelay) {
			return
		}
	}
}

// waitAndReconnect blocks until a working connection is established, or
// ctx is canceled. Returns false if it gave up because ctx was canceled.
func (c *rabbitMQConsumer) waitAndReconnect(ctx context.Context, delay time.Duration) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
		if err := c.connect(); err == nil {
			return true
		} else {
			slog.Error("rabbitmq reconnect failed", "error", err)
		}
	}
}

// consumeUntilClosed processes deliveries until the connection drops or ctx
// is canceled. Returns false if ctx was canceled (caller should stop for
// good), true if the connection dropped and a reconnect should be
// attempted.
func (c *rabbitMQConsumer) consumeUntilClosed(ctx context.Context, deliveries <-chan amqp.Delivery, connClosed <-chan *amqp.Error, handler Handler) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case amqpErr, ok := <-connClosed:
			if ok {
				slog.Error("rabbitmq connection closed, reconnecting", "error", amqpErr)
			}
			return true
		case d, ok := <-deliveries:
			if !ok {
				return true
			}
			select {
			case c.sem <- struct{}{}:
			case <-ctx.Done():
				return false
			}
			go c.handleDelivery(ctx, d, handler)
		}
	}
}

func (c *rabbitMQConsumer) handleDelivery(ctx context.Context, d amqp.Delivery, handler Handler) {
	defer func() { <-c.sem }()
	eventID, err := decode(d.Body)
	if err != nil {
		slog.Error("rabbitmq malformed envelope, dropping", "error", err)
		_ = d.Ack(false)
		return
	}
	attempts := rabbitDeliveryAttempts(d.Headers)
	delay := linearRequeueDelay(attempts, c.cfg.DefaultRequeueDelay, c.cfg.MaxRequeueDelay)
	if err := handler(ctx, Delivery{
		EventID: eventID, Attempts: attempts,
		MaxAttempts: c.cfg.MaxAttempts, RequeueDelay: delay,
		defaultRequeueDelay: c.cfg.DefaultRequeueDelay,
		maxRequeueDelay:     c.cfg.MaxRequeueDelay,
	}); err != nil {
		requestedDelay, _, _ := requestedRequeue(err, delay)
		slog.Warn("rabbitmq handler requested delayed requeue",
			"event_id", eventID, "attempts", attempts, "delay", requestedDelay, "error", err)
		go c.requeueAfter(ctx, d, eventID, attempts, requestedDelay, err)
		return
	}
	if err := d.Ack(false); err != nil {
		slog.Error("rabbitmq ack failed", "event_id", eventID, "attempts", attempts, "error", err)
	}
}

func (c *rabbitMQConsumer) requeueAfter(ctx context.Context, d amqp.Delivery, eventID uuid.UUID, attempts uint32, delay time.Duration, handlerErr error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return // closing the connection returns the unacked original to RabbitMQ
	case <-timer.C:
	}

	nextAttempts := attempts + 1
	if nextAttempts == 0 {
		nextAttempts = attempts
	}
	if err := c.publishRetry(ctx, d, eventID, nextAttempts); err != nil {
		slog.Error("rabbitmq delayed requeue publish failed; nacking original",
			"event_id", eventID, "attempts", attempts, "handler_error", handlerErr, "error", err)
		if nackErr := d.Nack(false, true); nackErr != nil {
			slog.Error("rabbitmq fallback nack failed", "event_id", eventID, "error", nackErr)
		}
		return
	}
	if err := d.Ack(false); err != nil {
		// The replacement was confirmed before this ACK. An ACK failure can
		// therefore produce a duplicate, which the database lease absorbs.
		slog.Warn("rabbitmq original ack failed after confirmed retry publish",
			"event_id", eventID, "attempts", attempts, "error", err)
		return
	}
	slog.Debug("rabbitmq event requeued",
		"event_id", eventID, "attempts", nextAttempts, "delay", delay)
}

func (c *rabbitMQConsumer) publishRetry(ctx context.Context, d amqp.Delivery, eventID uuid.UUID, attempts uint32) error {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	if c.retryCh == nil {
		return fmt.Errorf("rabbitmq retry channel is unavailable")
	}
	headers := cloneAMQPTable(d.Headers)
	headers[rabbitAttemptsHeader] = int64(attempts)
	confirmation, err := c.retryCh.PublishWithDeferredConfirmWithContext(ctx,
		c.exchange(), c.routingKey(), true, false,
		amqp.Publishing{
			Headers: headers, ContentType: d.ContentType, ContentEncoding: d.ContentEncoding,
			DeliveryMode: amqp.Persistent, Priority: d.Priority,
			CorrelationId: d.CorrelationId, ReplyTo: d.ReplyTo,
			MessageId: eventID.String(), Timestamp: d.Timestamp,
			Type: d.Type, UserId: d.UserId, AppId: d.AppId, Body: d.Body,
		})
	if err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}
	ok, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirm retry: %w", err)
	}
	if !ok {
		return fmt.Errorf("broker nacked retry publish")
	}
	return nil
}

func (c *rabbitMQConsumer) exchange() string {
	return c.cfg.Exchange
}

func (c *rabbitMQConsumer) routingKey() string {
	if c.cfg.Exchange == "" {
		return c.cfg.Queue
	}
	return c.cfg.RoutingKey
}

func rabbitDeliveryAttempts(headers amqp.Table) uint32 {
	if headers == nil {
		return 1
	}
	switch value := headers[rabbitAttemptsHeader].(type) {
	case int8:
		if value > 0 {
			return uint32(value)
		}
	case int16:
		if value > 0 {
			return uint32(value)
		}
	case int32:
		if value > 0 {
			return uint32(value)
		}
	case int64:
		if value > 0 && value <= int64(^uint32(0)) {
			return uint32(value)
		}
	case uint8:
		return uint32(value)
	case uint16:
		return uint32(value)
	case uint32:
		if value > 0 {
			return value
		}
	}
	return 1
}

func cloneAMQPTable(source amqp.Table) amqp.Table {
	clone := make(amqp.Table, len(source)+1)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (c *rabbitMQConsumer) Close() error {
	c.retryMu.Lock()
	if c.retryCh != nil {
		_ = c.retryCh.Close()
		c.retryCh = nil
	}
	c.retryMu.Unlock()
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
