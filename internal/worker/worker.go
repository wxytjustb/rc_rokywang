package worker

import (
	"context"
	"log/slog"

	"notification-delivery/internal/mq"
)

// Worker ties a delivery consumer, Processor and embedded Compensator into
// one runtime. External MQ instances can scale as competing consumers;
// memory mode runs one goroutine pool inside cmd/server. Both modes rely on
// the database's atomic UPDATE ... WHERE guards to consume duplicate wakes.
type Worker struct {
	Consumer    mq.Consumer
	Processor   *Processor
	Compensator *Compensator
	Concurrency int
	Logger      *slog.Logger
}

// Run starts the compensator loops and the MQ consumer, then blocks until
// ctx is canceled.
func (w *Worker) Run(ctx context.Context) error {
	w.Compensator.Run(ctx)

	if err := w.Consumer.Start(ctx, w.Concurrency, w.Processor.HandleDelivery); err != nil {
		return err
	}

	<-ctx.Done()
	return w.Consumer.Close()
}
