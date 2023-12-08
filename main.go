package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/consumer"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/producer"
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

func main() {
	cfg := config.NewKafkaCfg()
	kafka := NewKafka(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go kafka.MP.SendMessageWithRandomKey(ctx, 50)

	ctx2, cancel2 := context.WithCancel(context.Background())

	// Start the consumer in a separate goroutine
	go kafka.C.StartConsumer(ctx2)

	// Capture interrupt signals (Ctrl+C) to gracefully stop the consumer
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	// Wait for the interrupt signal
	<-stopSignal

	// Signal the consumer to stop by canceling the context
	cancel2()
}
