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
	// ── API layer

	// counts every HTTP request hitting your server
	// labels: method (POST/GET), path (/api/v1/notify), status (200/202/429/500)
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests received.",
	}, []string{"method", "path", "status"})

	// measures how long each HTTP request takes
	// labels: method, path
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	// counts requests currently being processed right now
	// goes up when request arrives, down when response sent
	HTTPRequestsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "Number of HTTP requests currently being processed.",
	})

	//  Notification pipeline

	NotificationsSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_sent_total",
		Help: "Total notifications successfully delivered.",
	}, []string{"channel"})

	NotificationsFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_failed_total",
		Help: "Total notification delivery failures.",
	}, []string{"channel"})

	NotificationsSkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_skipped_total",
		Help: "Notifications skipped (rate limited or opted out).",
	}, []string{"channel", "reason"})

	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_delivery_duration_seconds",
		Help:    "End-to-end delivery latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"channel"})

	KafkaPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_publish_total",
		Help: "Total Kafka publish attempts.",
	}, []string{"channel", "status"})

	//  Infrastructure

	// how far behind is your worker from the latest Kafka message
	KafkaConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Number of messages worker is behind in Kafka partition.",
	}, []string{"partition"})

	// number of active worker goroutines right now
	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "active_workers",
		Help: "Number of worker goroutines currently running.",
	})
)
