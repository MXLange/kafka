package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MXLange/kafka/consumer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &Process{
		name: "process message",
	}

	k := consumer.NewConsumerConfig()

	k.Topics = []string{"teste"}
	k.Function = p.processMessage
	k.BootstrapServers = "00.0.0.000:9094,00.0.0.000:9094"
	k.SecurityProtocol = "SASL_PLAINTEXT"
	k.SaslMechanism = "SCRAM-SHA-256"
	k.SaslUsername = "your_username"
	k.SaslPassword = "your_password"
	k.GroupId = "create_group_id"

	consumer, err := consumer.NewConsumer(k)
	if err != nil {
		log.Default().Panicf("Error creating consumer: %v\n", err)
	}

	err = consumer.Start(ctx)
	if err != nil {
		log.Default().Panicf("Error starting consumer: %v\n", err)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Default().Panicf("Error closing consumer: %v\n", err)
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
}

type Process struct {
	name string
}

func (p *Process) processMessage(message *kafka.Message) bool {

	log.Default().Printf("Processing message %v\n", p.name)

	log.Default().Printf("Offset: %v\n", message.TopicPartition.Offset)
	defer log.Default().Printf("Message %v processed\n", message)

	log.Default().Printf("Topic: %v\n", message.TopicPartition.Partition)
	log.Default().Printf("Key: %v\n", string(message.Key))
	log.Default().Printf("Value: %v\n", string(message.Value))

	return true
}
