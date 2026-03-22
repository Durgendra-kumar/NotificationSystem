package config

/*
Reads all settings from environment variables: server port, Kafka broker address,Postgres DSN, Redis address.
 Most importantly contains the Kafka routing table which channel maps to which topic and partition.
*/

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
// Values are read from environment variables never hardcoded.
type Config struct {
	Server   ServerConfig // Notification server config
	Kafka    KafkaConfig
	Postgres PostgresConfig
	Redis    RedisConfig
}

type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type KafkaConfig struct {
	BrokerAddr string
	Routing    map[string]ChannelRouting
}

// ChannelRouting maps one notification channel to its Kafka coordinates.
type ChannelRouting struct {
	Topic     string // topic name
	Partition int    //  partition number
	GroupID   string // consumer group id
}

type PostgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	CacheTTL time.Duration
}

// Load reads configuration from environment variables.
// Returns an error if any required variable is missing.
func Load() (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Port:         getEnv("SERVER_PORT", "8080"),
			ReadTimeout:  getDuration("SERVER_READ_TIMEOUT", 10*time.Second),
			WriteTimeout: getDuration("SERVER_WRITE_TIMEOUT", 10*time.Second),
		},
		Kafka: KafkaConfig{
			BrokerAddr: getEnv("KAFKA_BROKER", "localhost:9092"),
			// Using 1 Topic 4 partition.
			// Each channel gets its own partition within "notifications".
			// Partition number is the routing key.
			Routing: map[string]ChannelRouting{
				"ios":     {Topic: "notifications", Partition: 0, GroupID: "worker-ios"},
				"android": {Topic: "notifications", Partition: 1, GroupID: "worker-android"},
				"sms":     {Topic: "notifications", Partition: 2, GroupID: "worker-sms"},
				"email":   {Topic: "notifications", Partition: 3, GroupID: "worker-email"},
			},
		},
		Postgres: PostgresConfig{
			DSN:             getEnvRequired("POSTGRES_DSN"),
			MaxOpenConns:    getInt("POSTGRES_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getInt("POSTGRES_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: getDuration("POSTGRES_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getInt("REDIS_DB", 0),
			CacheTTL: getDuration("REDIS_CACHE_TTL", 5*time.Minute),
		},
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvRequired(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %s is not set", key))
	}
	return v
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
