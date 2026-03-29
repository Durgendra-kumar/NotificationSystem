package main

/*
Starts 4 worker goroutines — one per channel (iOS, Android, SMS, Email).
Each worker reads from its Kafka partition and delivers notifications.
Runs as a separate process from the API.
Why needed: delivery is async. The API just publishes to Kafka and returns.
Workers are the separate process that actually calls APNs, FCM, Twilio, SendGrid.
*/

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Durgendra-kumar/NotificationSystem/internal/config"
	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
	internalkafka "github.com/Durgendra-kumar/NotificationSystem/internal/kafka"
	"github.com/Durgendra-kumar/NotificationSystem/internal/sender"
	"github.com/Durgendra-kumar/NotificationSystem/internal/store"
	"github.com/Durgendra-kumar/NotificationSystem/internal/worker"
	"github.com/Durgendra-kumar/NotificationSystem/pkg/logger"
)

func main() {
	log := logger.New()

	if err := run(log); err != nil {
		log.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	//  Load config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	//  Connect to db: Postgres
	ctx := context.Background()
	db, err := store.New(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer db.Close()
	log.Info("worker: connected to postgres")

	//  Build one sender per channel
	// Each sender implements domain.Sender.
	// Swap MockSender for real implementations when API keys are ready.
	senders := map[domain.Channel]domain.Sender{
		domain.ChannelIOS:     sender.NewAPNSSender(log),
		domain.ChannelAndroid: sender.NewFCMSender(log),
		domain.ChannelSMS:     sender.NewSMSSender(log),
		domain.ChannelEmail:   sender.NewEmailSender(log),
	}

	// Create a cancellable context for all workers
	// When this context is cancelled (on shutdown signal), all workers
	// stop reading from Kafka and exit their Run() loop cleanly.
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	//  Start one worker goroutine per channel
	// WaitGroup tracks all workers - we wait for all to finish on shutdown.
	var wg sync.WaitGroup
	var workers []*worker.Worker

	for channel, sndr := range senders {
		// Create a Kafka consumer for this channel (reads from its partition/topic via config)
		consumer, err := internalkafka.NewConsumer(cfg.Kafka, channel)
		if err != nil {
			return fmt.Errorf("create consumer for %s: %w", channel, err)
		}

		// Wire the worker: consumer -> sender -> DB
		w := worker.New(consumer, sndr, db, log)
		workers = append(workers, w)

		// Launch the worker loop in its own goroutine
		wg.Add(1)
		go func(w *worker.Worker, ch domain.Channel) {
			defer wg.Done()
			log.Info("starting worker", "channel", ch)
			w.Run(workerCtx) // blocks until workerCtx is cancelled
			log.Info("worker stopped", "channel", ch)
		}(w, channel)
	}

	log.Info("all workers started", "count", len(workers))

	//  Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutdown signal received, stopping workers...")

	// Cancel the context — all worker Run() loops will exit
	cancelWorkers()

	// Wait for all workers to finish their current message before exiting
	wg.Wait()

	// Close all Kafka consumers (commits final offsets)
	for _, w := range workers {
		if err := w.Close(); err != nil {
			log.Error("error closing worker", "error", err)
		}
	}

	log.Info("all workers shut down cleanly")
	return nil
}
