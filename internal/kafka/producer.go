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

/*
Wraps kafka-go Writer. Publish(ctx, notification) looks up the correct topic and partition from config,
marshals the struct to JSON, writes to Kafka. Key is set to user_id so same user always goes to same partition.
Producer hides all Kafka mechanics — JSON encoding, partition routing, write timeouts — behind one clean method.
*/

// Producer publishes notifications to Kafka.
// It reads routing (topic + partition) from config — never hardcoded.
// Swapping from 1-topic-4-partitions to 4-topics is a config change only.
type Producer struct {
	writer  *kafka.Writer
	routing map[string]config.ChannelRouting
}

// NewProducer creates a Kafka producer using the provided config.
func NewProducer(cfg config.KafkaConfig) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BrokerAddr),
		Balancer:     &kafka.RoundRobin{},
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		// AllowAutoTopicCreation: true in dev — remove in production
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		writer:  writer,
		routing: cfg.Routing,
	}
}

// Publish sends one notification to the correct Kafka topic and partition.
// The routing is determined entirely by config — no channel-specific logic here.
// flow: Take go struct -> convert int Json -> create kafka msg -> kafka.Writer.WriteMessages into kafka
func (p *Producer) Publish(ctx context.Context, n *domain.Notification) error {
	route, ok := p.routing[string(n.Channel)]
	if !ok {
		return fmt.Errorf("publish: no routing config for channel %q", n.Channel)
	}

	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("publish: marshal notification: %w", err)
	}

	msg := kafka.Message{
		Topic:     route.Topic,
		Partition: route.Partition,
		Key:       []byte(n.UserID), // same user -> same partition -> ordered delivery
		Value:     data,
		Time:      time.Now(),
		Headers: []kafka.Header{
			{Key: "channel", Value: []byte(n.Channel)},
			{Key: "notification_id", Value: []byte(n.ID)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish: write to kafka [topic=%s partition=%d]: %w",
			route.Topic, route.Partition, err)
	}

	return nil
}

// Close flushes pending messages and closes the writer.
// Always call this on shutdown — unflushed messages are lost if skipped.
func (p *Producer) Close() error {
	return p.writer.Close()
}
