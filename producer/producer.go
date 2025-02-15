package producer

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mcuadros/go-defaults"
)

type Producer struct {
	configs  ProducerConfig
	producer *kafka.Producer
}

// ProducerConfig is a struct with the producer configs
type ProducerConfig struct {

	// Default: true - Defines if the producer is synchronous
	// If true, the producer waits for the message to be sent
	// If false, the producer sends the message asynchronously on batch mode
	Synchronous bool `default:"true"`

	// Default: empty - Broker address, e.g., localhost:9092
	BootstrapServers string `kafka:"bootstrap.servers"`

	// Default: empty - Username for authentication
	SaslUsername string `kafka:"sasl.username"`

	// Default: empty - Password for authentication
	SaslPassword string `kafka:"sasl.password"`

	// Default: PLAIN - PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, defines the authentication mechanism
	SaslMechanism string `kafka:"sasl.mechanism"`

	// Default: PLAINTEXT - SASL_SSL, SASL_PLAINTEXT, SSL, PLAINTEXT, defines the security protocol
	SecurityProtocol string `kafka:"security.protocol"`

	// Default: 100000 - Defines the maximum number of messages the producer waits to send
	QueueBufferingMaxMessages int `default:"100000" kafka:"queue.buffering.max.messages"`

	// Default: 1048576 (1GB) - Maximum size of the internal buffer in KB, 102400 (100MB)
	QueueBufferingMaxKBytes int `default:"1048576" kafka:"queue.buffering.max.kbytes"`

	// Default: 5000 (5s) - Defines the maximum time the producer waits to send the message
	QueueBufferingMaxMs int `default:"10000" kafka:"queue.buffering.max.ms"`

	// Default: 1000 - Defines the number of messages the producer waits to send in a batch
	BatchNumMessages int `default:"10000" kafka:"batch.num.messages"`

	// Default: 2 - Defines the number of retry attempts in case of failure
	MessageSendMaxRetries int `default:"2" kafka:"message.send.max.retries"`

	// Default: 100 - Defines the time in milliseconds the producer waits before trying to resend the message
	RetryBackoffMs int `default:"100" kafka:"retry.backoff.ms"`

	// Default: none - gzip, snappy, lz4, zstd, defines the compression type
	CompressionType string `default:"none" kafka:"compression.type"`

	// Default: 1 - 0 (no acknowledgment), 1 (leader acknowledgment), all/-1 (acknowledgment from all replicas)
	Acks int `default:"1" kafka:"acks"`

	// Default: false - true (ensures order and no duplicates), false (no guarantee of order or duplicates)
	EnableIdempotence bool `default:"false" kafka:"enable.idempotence"`

	// Default: 0 - Time in milliseconds the producer waits before sending the message
	LingerMs int `default:"0" kafka:"linger.ms"`

	// Default: 300000 (5min) - Total time the producer waits for message delivery, including retry attempts
	DeliveryTimeoutMs int `default:"300000" kafka:"delivery.timeout.ms"`

	// Default: 10000 (10s) - Time in milliseconds the producer waits for the broker to respond
	MessageTimeoutMs int `default:"10000" kafka:"message.timeout.ms"`

	// Default: 10 - Time in milliseconds the producer waits to send the message in case the buffer is full
	FlushTimeoutMs int `default:"2"`
}

// ToMap converts the Config struct to a map with string keys using the fieldname annotation.
func (cfg *ProducerConfig) toMap() *kafka.ConfigMap {
	result := &kafka.ConfigMap{}
	val := reflect.ValueOf(cfg).Elem()
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		tag := field.Tag.Get("kafka")
		if tag != "" {
			result.SetKey(tag, val.Field(i).Interface())
		}
	}
	return result
}

// NewProducerConfig creates a new producer config with the default values
func NewProducerConfig() *ProducerConfig {
	p := &ProducerConfig{}
	defaults.SetDefaults(p)
	return p
}

// NewProducer creates a new producer with the configs and returns a pointer to the producer
// if the connection fails, it returns an error
func NewProducer(configs *ProducerConfig) (*Producer, error) {
	p := &Producer{
		configs: *configs,
	}

	err := p.connect()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Close closes the producer connection
func (p *Producer) Close() {
	p.producer.Close()
}

func (p *Producer) connect() error {
	config := p.configs.toMap()

	var err error = nil

	p.producer, err = kafka.NewProducer(config)
	if err != nil {
		log.Default().Fatalf("fail to create producer: %s\n", err)
		return err
	}

	log.Default().Printf("producer created %v\n", p.producer.String())

	if !p.configs.Synchronous {
		go func() {
			for event := range p.producer.Events() {
				switch ev := event.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Default().Printf("error delivering message to topic %s [%d]: %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Error)
					} else {
						log.Default().Printf("message delivered to topic %s [%d] - offset: %d\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				case kafka.Error:
					log.Default().Printf("kafka error: %v\n", ev)
				}
			}
		}()
	}

	return nil
}

// SendMessage sends a message to a kafka topic
// if the producer is synchronous, it waits for the message to be sent and the error treatment is more detailed and precise
// if the producer is !synchronous, it sends the message asynchronously and the error treatment is less detailed, may lose messages
func (p *Producer) SendMessage(topic string, message, key []byte, header []kafka.Header) error {

	if p.configs.Synchronous {
		return p.sendMessageSync(topic, message, key, header)
	}

	return p.sendMessageAsync(topic, message, key, header)
}

// SendMessage sends a message to a kafka topic and waits for the message to be sent
func (p *Producer) sendMessageSync(topic string, message, key []byte, header []kafka.Header) error {

	if strings.ReplaceAll(topic, " ", "") == "" {
		return fmt.Errorf("topic is required")
	}

	if len(message) == 0 {
		return fmt.Errorf("message is required")
	}

	deliveryChan := make(chan kafka.Event)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
		Key:            key,
		Headers:        header,
	}, deliveryChan)

	unflushedMessages := p.producer.Flush(p.configs.FlushTimeoutMs) // The producer waits for the buffer to be empty
	log.Default().Printf("unflushed messages: %d\n", unflushedMessages)

	if err != nil {
		msg := strings.ToUpper(err.Error())
		if strings.Contains(msg, "QUEUE FULL") {
			log.Default().Printf("erro ao enviar mensagem para o tópico %s: %s\n", topic, err.Error())
			log.Default().Printf("staring to flush messages\n")
			unflushedMessages := p.producer.Flush(p.configs.FlushTimeoutMs * 5) // The producer waits for the buffer to be empty
			log.Default().Printf("unflushed messages: %d\n", unflushedMessages)
		}
		return err
	}

	event := <-deliveryChan
	switch ev := event.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			log.Default().Printf("error delivering message to topic %s [%d]: %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Error)
			return ev.TopicPartition.Error
		} else {
			log.Default().Printf("message delivered to topic %s [%d] - offset: %d\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
		}
	case kafka.Error:
		log.Default().Printf("kafka error: %v\n", ev)
		return ev
	}

	return nil
}

// SendMessage sends a message to a kafka topic and does not wait for the message to be sent
// it uses the batch mode, the performance is better, but the producer does not wait for the message to be sent
func (p *Producer) sendMessageAsync(topic string, message, key []byte, header []kafka.Header) error {

	if strings.ReplaceAll(topic, " ", "") == "" {
		return fmt.Errorf("topic is required")
	}

	if len(message) == 0 {
		return fmt.Errorf("message is required")
	}

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
		Key:            key,
		Headers:        header,
	}, nil)

	unflushedMessages := p.producer.Flush(p.configs.FlushTimeoutMs) // The producer waits for the buffer to be empty
	log.Default().Printf("unflushed messages: %d\n", unflushedMessages)

	if err != nil {
		msg := strings.ToUpper(err.Error())
		if strings.Contains(msg, "QUEUE FULL") {
			log.Default().Printf("erro ao enviar mensagem para o tópico %s: %s\n", topic, err.Error())
			log.Default().Printf("staring to flush messages\n")
			unflushedMessages := p.producer.Flush(p.configs.FlushTimeoutMs * 5) // The producer waits for the buffer to be empty
			log.Default().Printf("unflushed messages: %d\n", unflushedMessages)
		}
		return err
	}

	return nil
}
