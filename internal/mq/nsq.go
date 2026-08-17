package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	gonsq "github.com/nsqio/go-nsq"

	"notification-delivery/internal/config"
)

// --- publisher -------------------------------------------------

type nsqPublisher struct {
	producer *gonsq.Producer
	topic    string
}

func newNSQPublisher(cfg config.NSQConfig) (*nsqPublisher, error) {
	producer, err := gonsq.NewProducer(cfg.NSQDAddr, gonsq.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("nsq producer: %w", err)
	}
	return &nsqPublisher{producer: producer, topic: cfg.Topic}, nil
}

func (p *nsqPublisher) Publish(ctx context.Context, eventID uuid.UUID) (PublishResult, error) {
	body, err := encode(eventID)
	if err != nil {
		return PublishResult{}, err
	}
	if err := p.producer.Publish(p.topic, body); err != nil {
		return PublishResult{}, fmt.Errorf("nsq publish: %w", err)
	}
	return PublishResult{Durable: true}, nil
}

func (p *nsqPublisher) Close() error {
	p.producer.Stop()
	return nil
}

// --- consumer -------------------------------------------------

type nsqConsumer struct {
	consumer            *gonsq.Consumer
	lookupdAddrs        []string
	defaultRequeueDelay time.Duration
	maxRequeueDelay     time.Duration
	maxAttempts         uint32
}

func newNSQConsumer(cfg config.NSQConfig) (*nsqConsumer, error) {
	if err := validateRequeueConfig("nsq", cfg.RequeueConfig); err != nil {
		return nil, err
	}
	nsqConfig := gonsq.NewConfig()
	nsqConfig.DefaultRequeueDelay = cfg.DefaultRequeueDelay
	nsqConfig.MaxRequeueDelay = cfg.MaxRequeueDelay
	// Keep go-nsq's pre-handler cutoff disabled. The configured max_attempts
	// is delivered to Worker, which writes PostgreSQL's terminal FAILED state
	// on the final attempt before returning nil and allowing go-nsq to FIN.
	nsqConfig.MaxAttempts = 0
	consumer, err := gonsq.NewConsumer(cfg.Topic, cfg.Channel, nsqConfig)
	if err != nil {
		return nil, fmt.Errorf("nsq consumer: %w", err)
	}
	return &nsqConsumer{
		consumer: consumer, lookupdAddrs: cfg.LookupdAddrs,
		defaultRequeueDelay: nsqConfig.DefaultRequeueDelay,
		maxRequeueDelay:     nsqConfig.MaxRequeueDelay,
		maxAttempts:         cfg.MaxAttempts,
	}, nil
}

type nsqHandlerFunc struct {
	ctx                 context.Context
	handler             Handler
	defaultRequeueDelay time.Duration
	maxRequeueDelay     time.Duration
	maxAttempts         uint32
}

func (h *nsqHandlerFunc) HandleMessage(m *gonsq.Message) error {
	eventID, err := decode(m.Body)
	if err != nil {
		// Malformed body can never succeed on redelivery either; log and
		// drop rather than requeue forever. This should not happen in
		// practice since this service is the only publisher.
		return nil
	}
	fallbackDelay := linearRequeueDelay(uint32(m.Attempts), h.defaultRequeueDelay, h.maxRequeueDelay)
	err = h.handler(h.ctx, Delivery{
		EventID:             eventID,
		Attempts:            uint32(m.Attempts),
		MaxAttempts:         h.maxAttempts,
		RequeueDelay:        fallbackDelay,
		defaultRequeueDelay: h.defaultRequeueDelay,
		maxRequeueDelay:     h.maxRequeueDelay,
	})
	if err == nil {
		return nil
	}
	delay, backoff, explicit := requestedRequeue(err, fallbackDelay)
	if !explicit {
		return err
	}
	if backoff {
		m.Requeue(delay)
	} else {
		m.RequeueWithoutBackoff(delay)
	}
	return nil
}

func (c *nsqConsumer) Start(ctx context.Context, concurrency int, handler Handler) error {
	h := &nsqHandlerFunc{
		ctx: ctx, handler: handler,
		defaultRequeueDelay: c.defaultRequeueDelay,
		maxRequeueDelay:     c.maxRequeueDelay,
		maxAttempts:         c.maxAttempts,
	}
	if concurrency > 1 {
		c.consumer.AddConcurrentHandlers(h, concurrency)
	} else {
		c.consumer.AddHandler(h)
	}
	if err := c.consumer.ConnectToNSQLookupds(c.lookupdAddrs); err != nil {
		return fmt.Errorf("nsq connect to lookupd: %w", err)
	}
	go func() {
		<-ctx.Done()
		c.consumer.Stop()
	}()
	return nil
}

func (c *nsqConsumer) Close() error {
	c.consumer.Stop()
	<-c.consumer.StopChan
	return nil
}
