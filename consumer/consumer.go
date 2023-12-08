package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/counter"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/producer"
	"github.com/segmentio/kafka-go"
)

// KafkaCfg represents Kafka configuration.

// Consumer represents a Kafka message consumer.
type Consumer struct {
	reader          *kafka.Reader
	deadTopicWriter *producer.Producer
	count           counter.MessageCounter
}

func (c *Consumer) GetMessageCount() int {
	return c.count.GetMessageCount()
}

func (c *Consumer) IncrementCount() {
	c.count.Increment()
}

// NewConsumer creates a new instance of the Consumer with the provided Kafka configuration.
func NewConsumer(cfg config.KafkaCfg, dtw *producer.Producer) *Consumer {
	brokers := []string{fmt.Sprintf("%s:%s", cfg.GetHost(), cfg.GetPort())}
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    cfg.GetTopic(),
			GroupID:  cfg.GetReaderGroupId(),
			MinBytes: 10,
			MaxBytes: 1e6,
			// MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
			StartOffset:    kafka.LastOffset,
		}),
		deadTopicWriter: dtw,
	}
}

// StartConsumer starts the Kafka message consumer.
func (c *Consumer) StartConsumer(ctx context.Context) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				log.Println("Error fetching message:", err)
				continue
			}

			// Process the received message
			if flag := c.processMessage(msg); !flag {
				continue
			}

			// Commit the message offset
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Println("Error committing message offset:", err)
			}
		}
	}
}

// StopConsumer stops the Kafka message consumer.
func (c *Consumer) StopConsumer() {
	if err := c.reader.Close(); err != nil {
		log.Println("Error closing Kafka reader:", err)
	}
}

func (c *Consumer) processMessage(msg kafka.Message) bool {
	if msg.Value == nil {
		fmt.Println("Message Value is nil")
		return true
	}

	messageValue := string(msg.Value)

	// Check if the string contains any non-alphanumeric characters
	if containsNonAlphanumeric(messageValue) {
		fmt.Printf("Message contains non-alphanumeric characters: %s\n", messageValue)

		// Write the message to the dead letter topic
		err := c.deadTopicWriter.SendMessage(context.TODO(), string(msg.Key), messageValue)
		if err != nil {
			fmt.Printf("Failed to write message to dead letter topic: %v\n", err)
			return false
		}
		c.deadTopicWriter.IncrementCount()
		return true
	}

	fmt.Printf("Received message: %s\n", messageValue)
	c.IncrementCount()
	return true
}

func containsNonAlphanumeric(s string) bool {
	// Use a regular expression to check if the string contains any non-alphanumeric characters
	regex := regexp.MustCompile("[^a-zA-Z0-9]+")
	return regex.MatchString(s)
}
