package tests

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/consumer"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/producer"
	"gotest.tools/assert"
)

type Kafka struct {
	C  *consumer.Consumer
	MP *producer.Producer
	DP *producer.Producer
}

func NewKafka(cfg config.KafkaCfg) *Kafka {
	mainProducer := producer.NewProducer(cfg, cfg.GetTopic())
	deadTopicProducer := producer.NewProducer(cfg, cfg.GetDLQTopic())
	consumer := consumer.NewConsumer(cfg, deadTopicProducer)
	return &Kafka{
		MP: mainProducer,
		DP: deadTopicProducer,
		C:  consumer,
	}
}

func TestMessageProcessing(t *testing.T) {
	testCases := []struct {
		Messages int
		Duration time.Duration
	}{
		{Messages: 5, Duration: 10 * time.Second},
		{Messages: 15, Duration: 30 * time.Second},
		{Messages: 20, Duration: 40 * time.Second},
		{Messages: 40, Duration: 80 * time.Second},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("Test : %d", idx+1), func(t *testing.T) {
			// Set up your Kafka configuration for testing
			cfg := config.NewKafkaCfg()

			// Create a Kafka instance with the test configuration
			kafka := NewKafka(cfg)

			// Start producing and consuming messages in separate goroutines
			go kafka.MP.SendMessageWithRandomKey(context.Background(), tc.Messages)
			go kafka.C.StartConsumer(context.Background())

			// Wait for the specified duration
			time.Sleep(tc.Duration)
			kafka.C.StopConsumer()

			// Retrieve message counts
			producedMessageCount := kafka.MP.GetMessageCount()
			deadMessageCount := kafka.DP.GetMessageCount()
			processedMessageCount := kafka.C.GetMessageCount()

			// Log message counts
			log.Println(producedMessageCount, deadMessageCount, processedMessageCount)

			// Assert the conditions for the test case
			assert.Equal(t, producedMessageCount, deadMessageCount+processedMessageCount)
		})
	}
}
