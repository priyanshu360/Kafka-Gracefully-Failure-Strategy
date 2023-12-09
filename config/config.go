package config

import "os"

type KafkaCfg struct {
	host          string
	port          string
	topic         string
	dlqTopic      string
	readerGroupId string
}

// NewKafkaCfg creates a new KafkaCfg instance with the provided values.
func NewKafkaCfg() KafkaCfg {
	return KafkaCfg{
		host:          getEnv("KAFKA_HOST", "localhost"),
		port:          getEnv("KAFKA_PORT", "29092"),
		topic:         getEnv("KAFKA_TOPIC", "main_topic"),
		dlqTopic:      getEnv("KAFKA_DLQ_TOPIC", "dead_letter_topic"),
		readerGroupId: getEnv("READER_GROUP_ID", "reader_group"),
	}
}

func NewKafkaCfgWithPort(port string) KafkaCfg {
	return KafkaCfg{
		host:          getEnv("KAFKA_HOST", "localhost"),
		port:          port,
		topic:         getEnv("KAFKA_TOPIC", "main_topic"),
		dlqTopic:      getEnv("KAFKA_DLQ_TOPIC", "dead_letter_topic"),
		readerGroupId: getEnv("READER_GROUP_ID", "reader_group"),
	}
}

func getEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

// GetHost returns the host of the Kafka configuration.
func (cfg KafkaCfg) GetHost() string {
	return cfg.host
}

func (cfg KafkaCfg) GetReaderGroupId() string {
	return cfg.readerGroupId
}

// GetPort returns the port of the Kafka configuration.
func (cfg KafkaCfg) GetPort() string {
	return cfg.port
}

// GetTopic returns the topic of the Kafka configuration.
func (cfg KafkaCfg) GetTopic() string {
	return cfg.topic
}

// GetDLQTopic returns the dead-letter queue (DLQ) topic of the Kafka configuration.
func (cfg KafkaCfg) GetDLQTopic() string {
	return cfg.dlqTopic
}
