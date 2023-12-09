package tests

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/config"
	"github.com/priyanshu360/Kafka-Gracefully-Failure-Strategy/kafka"
	"gotest.tools/assert"
)

type testConfig struct {
	kafkaCfg               config.KafkaCfg
	maxMessageCount        int
	maxProduceTime         time.Duration
	maxConsumptionTime     time.Duration
	maxNetworkFluctianTime time.Duration
	containerName          string
}

func TestMessageProcessing(t *testing.T) {
	kafkaCfg1 := config.NewKafkaCfgWithPort("29092")
	kafkaCfg2 := config.NewKafkaCfgWithPort("29093")
	kafkaCfg3 := config.NewKafkaCfgWithPort("29094")

	testCases := []testConfig{
		{
			kafkaCfg:               kafkaCfg1,
			maxMessageCount:        100,
			maxProduceTime:         2 * time.Minute,
			maxConsumptionTime:     2 * time.Minute,
			maxNetworkFluctianTime: 0 * time.Second,
			containerName:          "tests-kafka-1-1",
		},
		{
			kafkaCfg:               kafkaCfg2,
			maxMessageCount:        50,
			maxProduceTime:         2 * time.Minute,
			maxConsumptionTime:     4 * time.Minute,
			maxNetworkFluctianTime: 3 * time.Minute,
			containerName:          "tests-kafka-2-1",
		},
		{
			kafkaCfg:               kafkaCfg3,
			maxMessageCount:        70,
			maxProduceTime:         1 * time.Minute,
			maxConsumptionTime:     4 * time.Minute,
			maxNetworkFluctianTime: 4 * time.Second,
			containerName:          "tests-kafka-3-1",
		},
	}

	for idx, tc := range testCases {
		exec.Command("docker", "start", "poc-kafka-kafka-1-1").Run()
		t.Run(fmt.Sprintf("Test : %d", idx+1), func(t *testing.T) {
			runKafkaTest(t, tc)
		})
	}
}

func runKafkaTest(t *testing.T, tc testConfig) {

	// Set up your Kafka configuration for testing

	// Create a Kafka instance with the test configuration
	k := kafka.NewKafka(tc.kafkaCfg)
	kafka.CreateTopic(tc.kafkaCfg)

	ctxNP, cancelNP := context.WithTimeout(context.Background(), tc.maxNetworkFluctianTime)
	go kafkaUpDown(ctxNP, tc.containerName)
	defer cancelNP()

	// Start producing and consuming messages in separate goroutines
	ctx, cancel := context.WithTimeout(context.Background(), tc.maxProduceTime)
	go k.MP.SendMessageWithRandomKey(ctx, tc.maxMessageCount)
	defer cancel()

	go k.C.StartConsumer(context.Background())

	// Wait for the specified duration
	time.Sleep(tc.maxConsumptionTime)
	k.C.StopConsumer()

	// Retrieve message counts
	producedMessageCount, producedMessageSet := k.MP.GetMessageCount()
	deadMessageCount, deadMessageSet := k.DP.GetMessageCount()
	processedMessageCount, processedMessageSet := k.C.GetMessageCount()

	// Log message counts
	log.Println(producedMessageCount, deadMessageCount, processedMessageCount)

	// Assert the conditions for the test case
	assert.Assert(t, producedMessageCount <= deadMessageCount+processedMessageCount)
	// log.Println(mergeMaps(*deadMessageSet, *processedMessageSet), *producedMessageSet)
	assert.Assert(t, reflect.DeepEqual(*producedMessageSet, mergeMaps(*deadMessageSet, *processedMessageSet)))
}

func kafkaUpDown(ctx context.Context, containerName string) {
	log.Println("kafkaUpDown")
	for {
		select {
		case <-ctx.Done():
			// Context canceled or timed out, stop the routine
			return
		default:
			log.Println("kafkaUpDown")
			exec.Command("docker", "stop", containerName).Run()
			time.Sleep(1 * time.Second)
			exec.Command("docker", "start", containerName).Run()
			time.Sleep(60 * time.Second)
		}
	}
}

func mergeMaps(map1, map2 map[string]bool) map[string]bool {
	result := make(map[string]bool)

	// Copy the first map
	for key, value := range map1 {
		result[key] = value
	}

	// Copy the second map, overwriting values if keys already exist
	for key, value := range map2 {
		result[key] = value
	}

	return result
}
