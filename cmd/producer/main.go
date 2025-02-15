package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/MXLange/kafka/producer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Handler struct {
	producer *producer.Producer
}

func main() {

	pc := producer.NewProducerConfig()
	pc.BootstrapServers = "00.0.0.000:9094,00.0.0.000:9094"
	pc.SaslUsername = "your_username"
	pc.SaslPassword = "touy_password"
	pc.SaslMechanism = "SCRAM-SHA-256"
	pc.SecurityProtocol = "SASL_PLAINTEXT"
	// pc.Synchronous = false

	p, err := producer.NewProducer(pc)
	if err != nil {
		log.Default().Fatalf("error creating producer: %v", err)
	}
	defer p.Close()

	h := &Handler{
		producer: p,
	}
	for i := 0; i < 100; i++ {
		value := "value " + strconv.Itoa(i)

		err = h.Send("test", "key", value, nil)
		if err != nil {
			log.Default().Fatalf("error sending message: %v", err)
		}
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
}

func (h *Handler) Send(topic string, key string, value string, headers []kafka.Header) error {

	err := h.producer.SendMessage(topic, []byte(value), []byte(key), headers)
	if err != nil {
		log.Default().Fatalf("error sending message: %v", err)
		return err
	}

	return nil
}
