package main

/*
Starts the HTTP API server on port 8080. Connects to Postgres, Redis, Kafka.
Wires all dependencies together. Listens for SIGTERM to shut down cleanly.
Why needed: every Go program needs a main().
 This is the entry point for the API process the thing other microservices call to send notifications.
*/

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/internal/api"
	"github.com/Durgendra-kumar/NotificationSystem/internal/config"
	"github.com/Durgendra-kumar/NotificationSystem/internal/kafka"
	"github.com/Durgendra-kumar/NotificationSystem/internal/ratelimit"
	"github.com/Durgendra-kumar/NotificationSystem/internal/store"
	"github.com/Durgendra-kumar/NotificationSystem/pkg/logger"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := logger.New()

	if err := run(log); err != nil {
		log.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// run separates startup logic from main() so errors can be returned cleanly.
// This is a common Go pattern — main() is just a thin wrapper around run().
func run(log *slog.Logger) error {
	//  Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	//  Connect to PostgreSQL
	db, err := store.New(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer db.Close()
	log.Info("connected to postgres")

	// Run schema migrations on startup
	if err := db.Migrate(ctx); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	log.Info("migrations applied")

	//  Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer rdb.Close()
	log.Info("connected to redis")

	//  Build dependencies
	userCache := store.NewUserCache(rdb, db, cfg.Redis.CacheTTL)
	limiter := ratelimit.NewLimiter(rdb, 100, time.Hour) // 100 per channel per hour
	producer := kafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	//  Wire HTTP handler and router
	handler := api.NewHandler(producer, userCache, db, limiter, log)
	router := api.NewRouter(handler, log)

	//  Start HTTP server ─
	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Run server in a goroutine so we can wait for shutdown signals below
	serverErr := make(chan error, 1)
	go func() {
		log.Info("api server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("server listen: %w", err)
		}
		close(serverErr)
	}()

	//  Wait for shutdown signal
	// Block here until SIGINT (Ctrl+C) or SIGTERM (Kubernetes pod shutdown)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return err
	case sig := <-quit:
		log.Info("shutdown signal received", "signal", sig)
	}

	//  Graceful shutdown
	// Give in-flight requests 10 seconds to complete before forcing close.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	log.Info("server shut down cleanly")
	return nil
}
