package producer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/counter"
	"github.com/segmentio/kafka-go"
)

// Producer represents a Kafka message producer.
type Producer struct {
	writer *kafka.Writer
	count  counter.MessageCounter
}

func (p *Producer) GetMessageCount() (int, *map[string]bool) {
	return p.count.GetMessageCount()
}

func (p *Producer) IncrementCount(message string) {
	p.count.Increment(message)
}

// NewProducer creates a new instance of the Producer with the provided Kafka configuration.
func NewProducer(cfg config.KafkaCfg, topic string) *Producer {
	brokers := []string{fmt.Sprintf("%s:%s", cfg.GetHost(), cfg.GetPort())}
	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        topic,
			BatchSize:    0,
			BatchTimeout: 500 * time.Millisecond,
		}),
		count: counter.NewMessageCounter(),
	}
}

// SendMessage sends a message to the Kafka topic.
func (p *Producer) SendMessage(ctx context.Context, key, value string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	return p.writer.WriteMessages(ctx, msg)
}

// CloseProducer closes the Kafka producer.
func (p *Producer) CloseProducer() {
	if err := p.writer.Close(); err != nil {
		log.Println("Error closing Kafka writer:", err)
	}
}
func (p *Producer) SendMessageWithRandomKey(ctx context.Context, maxItirations int) {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	for i := 0; i < maxItirations; i++ {
		select {
		case <-ctx.Done():
			// Context timed out, stop sending messages
			log.Println("Stopping message generation.")
			p.CloseProducer()
			return
		default:
			// Generate a random alphanumeric key
			randomKey := generateRandomKey(rng, 10)

			// Generate a random alphanumeric message
			randomMessage := generateRandomKey(rng, 10)

			// Send the message to the Kafka topic
			err := p.SendMessage(ctx, randomKey, randomMessage)
			if err != nil {
				// Handle the error (e.g., log it)
				log.Println("Error sending message:", err)
				time.Sleep(5 * time.Second)
			} else {
				p.IncrementCount(randomMessage)
			}

			// Sleep for a short duration (e.g., 1 second) before sending the next message
			time.Sleep(1 * time.Second)
		}
	}
	log.Println("Stopping message generation.")
}

// generateRandomKey generates a random alphanumeric string of the specified length.
func generateRandomKey(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789~!@#$%^&*()+abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	keyBytes := make([]byte, length)

	for i := range keyBytes {
		keyBytes[i] = charset[r.Intn(len(charset))]
	}

	return string(keyBytes)
}
