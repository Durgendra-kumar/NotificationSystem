package metrics

/*
Declares Prometheus metrics: notifications_sent_total (counter per channel), notifications_failed_total (counter per channel), notification_delivery_duration_seconds (histogram), kafka_publish_total (counter).
Why needed: these are the numbers that become your resume bullet points — "p99 latency under 150ms", "94% failure reduction". Without metrics you have no numbers, only claims.
*/
/*
Read it. Understand: counter vs histogram. What does each metric measure? When does it increment?
Goal: these files have zero dependencies on the rest of the project. Perfect starting point for writing code.
*/

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// NotificationsSentTotal counts successful deliveries by channel.
	NotificationsSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_sent_total",
		Help: "Total number of notifications successfully sent.",
	}, []string{"channel"})

	// NotificationsFailedTotal counts failed delivery attempts by channel.
	NotificationsFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_failed_total",
		Help: "Total number of notification delivery failures.",
	}, []string{"channel"})

	// NotificationsSkippedTotal counts skipped notifications (opted out, rate limited).
	NotificationsSkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_skipped_total",
		Help: "Total number of notifications skipped (user opted out or rate limited).",
	}, []string{"channel", "reason"})

	// DeliveryDuration measures end-to-end delivery latency per channel.
	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_delivery_duration_seconds",
		Help:    "End-to-end delivery latency from worker receive to provider ack.",
		Buckets: prometheus.DefBuckets,
	}, []string{"channel"})

	// KafkaPublishTotal counts Kafka publish attempts from the API server.
	KafkaPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_publish_total",
		Help: "Total Kafka publish attempts from the API server.",
	}, []string{"channel", "status"})
)
