package kafka

import (
	"fmt"
	"net"
	"strconv"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/kafka/consumer"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/kafka/producer"
	"github.com/segmentio/kafka-go"
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

func CreateTopic(cfg config.KafkaCfg) {
	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%s", cfg.GetHost(), cfg.GetPort()))
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{Topic: cfg.GetTopic(), NumPartitions: 3, ReplicationFactor: 1},
		{Topic: cfg.GetDLQTopic(), NumPartitions: 1, ReplicationFactor: 1},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}
