package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/internal/config"
	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
	"github.com/segmentio/kafka-go"
)

// Consumer reads notifications from one Kafka partition.
// Each channel worker (iOS, Android, SMS, Email) gets its own Consumer instance,
// pointed at its own partition/topic via config.
type Consumer struct {
	reader  *kafka.Reader
	channel domain.Channel
}

// NewConsumer creates a consumer for one specific channel.
// The topic and partition come from config — not hardcoded here.
func NewConsumer(cfg config.KafkaConfig, channel domain.Channel) (*Consumer, error) {
	route, ok := cfg.Routing[string(channel)]
	if !ok {
		return nil, fmt.Errorf("consumer: no routing config for channel %q", channel)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BrokerAddr},
		Topic:          route.Topic,
		GroupID:        route.GroupID,
		MinBytes:       1,                // fetch as soon as any message is available
		MaxBytes:       1 << 20,          // 1 MB max per fetch
		CommitInterval: time.Second,      // auto-commit offsets every second
		StartOffset:    kafka.LastOffset, // only new messages on fresh start
	})

	return &Consumer{
		reader:  reader,
		channel: channel,
	}, nil
}

// ReadOne blocks until a message arrives, then decodes and returns it.
// Returns context.Canceled when ctx is cancelled — the signal to stop.
// This is the inner loop of every worker: call ReadOne, process, repeat.
// flow : reader form particular partition -> convert json to Go struction -> return go struct
func (c *Consumer) ReadOne(ctx context.Context) (*domain.Notification, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("consumer [%s]: read message: %w", c.channel, err)
	}

	var n domain.Notification
	if err := json.Unmarshal(msg.Value, &n); err != nil {
		// Bad JSON in the message — this is a producer bug, not a transient error.
		// Return a wrapped error so the caller can route it to the DLQ.
		return nil, fmt.Errorf("consumer [%s]: unmarshal message offset=%d: %w",
			c.channel, msg.Offset, err)
	}

	return &n, nil
}

// Close shuts down the reader and commits the final offset.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
