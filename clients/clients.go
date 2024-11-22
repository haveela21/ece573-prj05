package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"context"

	"github.com/IBM/sarama"
)

func main() {
	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatalf("Unknown topic")
	}

	role := os.Getenv("ROLE")

	broker := os.Getenv("KAFKA_BROKER")

	if role == "producer" {
		producer(broker, topic)
	} else if role == "consumer" {
		consumer(broker, topic)
	} else {
		log.Fatalf("Unknown role %s", role)
	}
}

func producer(broker, topic string) {
	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		log.Fatalf("Cannot create producer at %s: %v", broker, err)
	}
	defer producer.Close()

	log.Printf("%s: start publishing messages to %s", topic, broker)
	for count := 1; ; count++ {
		value := rand.Float64()
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%f", value)),
		}

		_, _, err = producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Cannot publish message %d (%f) to %s: %v",
				count, value, topic, err)
		}

		if count%1000 == 0 {
			log.Printf("%s: %d messages published", topic, count)
		}
	}
}

func consumer(broker, topic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Define a consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{broker}, "example-group", config)
	if err != nil {
		log.Fatalf("Cannot create consumer group at %s: %v", broker, err)
	}
	defer consumerGroup.Close()

	// Create a new ConsumerGroupHandler instance
	consumer := &ConsumerGroupHandler{}

	// Start consuming messages using a valid context
	for {
		// Use a background context to run the consumer loop
		err := consumerGroup.Consume(context.Background(), []string{topic}, consumer)
		if err != nil {
			log.Fatalf("Error during consumption: %v", err)
		}
	}
}

type ConsumerGroupHandler struct{}

// Setup is run before consuming any messages.
func (c *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup is run after all messages have been consumed.
func (c *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim processes a batch of messages.
func (c *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message claimed: topic=%s, partition=%d, offset=%d, value=%s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		// Mark the message as processed
		sess.MarkMessage(msg, "")
	}
	return nil
}
