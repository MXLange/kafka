package consumer

import (
	"context"
	"log"
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mcuadros/go-defaults"
)

// ConsumerConfig contains the configuration for the Kafka consumer
type ConsumerConfig struct {
	// Default "mandatory": A list of topics to subscribe to (e.g., []string{"topic1", "topic2"}).
	Topics []string

	// Default "mandatory": A function that processes a Kafka message.
	// The function should return a boolean indicating if the message should be committed.
	Function func(*kafka.Message) bool

	// Default 1: The number of retries the consumer will attempt if a commit fails.
	// This is useful for handling transient errors and ensuring that offsets are committed successfully.
	CommitsRetry int `default:"1"`

	// Default "mandatory": A comma-separated list of Kafka broker addresses (e.g., "broker1:9092,broker2:9092").
	// Specifies the Kafka brokers to connect to for bootstrapping the consumer. This is a mandatory field and must be provided.
	BootstrapServers string `kafka:"bootstrap.servers"`

	// Default "mandatory": A valid SASL username (e.g., "user").
	// The username used for SASL authentication. This is a mandatory field if using SASL authentication.
	SaslUsername string `kafka:"sasl.username"`

	// Default "mandatory": A valid SASL password (e.g., "password").
	// The password associated with the SaslUsername for SASL authentication. This is a mandatory field if using SASL authentication.
	SaslPassword string `kafka:"sasl.password"`

	// Default "mandatory": A SASL mechanism (e.g., "PLAIN", "SCRAM-SHA-256").
	// Specifies the SASL mechanism used for authentication. The possible values depend on the Kafka server configuration.
	SaslMechanism string `kafka:"sasl.mechanism"`

	// Default "mandatory": Specifies the security protocol used for communication between the consumer and broker.
	// Possible values: "plaintext", "ssl", "sasl_plaintext", "sasl_ssl".
	// "plaintext": No encryption or authentication. "ssl": Uses SSL encryption without authentication.
	// "sasl_plaintext": Uses SASL authentication without SSL encryption. "sasl_ssl": Uses SASL authentication with SSL encryption.
	SecurityProtocol string `kafka:"security.protocol"`

	// Default "mandatory": A valid string representing the consumer group ID (e.g., "my-consumer-group").
	// Specifies the consumer group ID to which this consumer belongs. This is a mandatory field.
	GroupId string `kafka:"group.id"`

	// Default 6000: The time (in milliseconds) the consumer will wait before it is considered dead if it does not send heartbeats.
	// A value too high may lead to slow detection of consumer failures; too low may cause frequent rebalances.
	SessionTimeoutMs int `default:"6000" kafka:"session.timeout.ms"`

	// Default 600000: The maximum time (in milliseconds) allowed between consecutive calls to `poll()`.
	// If exceeded, the consumer will be considered out of the group and removed.
	MaxPoolIntervalMs int `default:"600000" kafka:"max.poll.interval.ms"`

	// Default "earliest": The offset to use if no offset is found for a partition or if the offset is out of range.
	// Possible values: "earliest", "latest", "error".
	// "earliest": Starts reading from the earliest available message. "latest": Starts reading from the latest message.
	// "error": Returns an error if the offset is out of range.
	AutoOffsetReset string `default:"earliest" kafka:"auto.offset.reset"`

	// Default false: Determines if the consumer should automatically commit offsets after fetching messages.
	// true: Automatically commits offsets. false: Manual control over committing offsets.
	EnableAutoCommit bool `default:"false" kafka:"enable.auto.commit"`

	// Default 200 (ms): The interval (in milliseconds) at which the consumer will commit offsets.
	AutoCommitIntervalMs int `default:"200" kafka:"auto.commit.interval.ms"`

	// Default false: Determines if offsets should be automatically stored after they are fetched.
	// true: Offsets are automatically stored after being fetched. false: Offsets are not automatically stored and must be manually managed.
	EnableAutoOffsetStore bool `default:"false" kafka:"enable.auto.offset.store"`

	// Default 100000: The minimum number of messages that must be in the queue before another fetch is initiated.
	// This helps with performance optimizations and controlling the fetching rate.
	QueuedMinMessages int `default:"100000" kafka:"queued.min.messages"`

	// Default 100: The maximum time (in milliseconds) the consumer will wait for additional messages before completing the fetch request.
	FetchWaitMaxMs int `default:"100" kafka:"fetch.wait.max.ms"`

	// Default 60000: The interval (in milliseconds) at which the consumer will send statistics about its operation to the broker.
	StatisticsIntervalMs int `default:"60000" kafka:"statistics.interval.ms"`

	// Default 1000: The interval (in milliseconds) at which the consumer sends heartbeats to the broker to maintain its session.
	HeartbeatIntervalMs int `default:"1000" kafka:"heartbeat.interval.ms"`

	// Default 100 (ms): The frequency (in milliseconds) at which the consumer's poll function is called.
	// This is the maximum time that the poll function can block for.
	PoolEventsTimeMs int `default:"100"`

	// Default 1: The max number of restarts in erro case
	MaxRestarts int `default:"1"`

	// Controls the for loop that consumes messages
	IsRunning bool `default:"true"`
}

// Consumer represents a Kafka consumer connection
type Consumer struct {
	consumer *kafka.Consumer
	config   *ConsumerConfig
}

// ToMap converts the Config struct to a map with string keys using the fieldname annotation.
func (cfg *ConsumerConfig) toMap() *kafka.ConfigMap {
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

func NewConsumerConfig() *ConsumerConfig {
	c := &ConsumerConfig{}
	defaults.SetDefaults(c)
	return c
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	cMap := config.toMap()

	c, err := kafka.NewConsumer(cMap)
	if err != nil {
		log.Default().Printf("Failed to create consumer: %s\n", err)
		return nil, err
	}

	log.Default().Printf("Consumer created %v\n", c.String())

	return &Consumer{consumer: c, config: config}, nil
}

// Start starts the Kafka consumer
// ctx context.Context is used to control the consumer lifecycle
func (c *Consumer) Start(ctx context.Context) error {
	err := c.subscribeTopics(c.config.Topics)
	if err != nil {
		log.Default().Printf("Failed to connect topics: %s\n", err)
		return err
	}

	c.consumeMessages(ctx, c.config.Function)

	return nil
}

// Close closes the Kafka consumer connection
func (c *Consumer) Close() error {
	err := c.consumer.Close()
	if err != nil {
		log.Default().Printf("Failed to Close consumer: %s\n", err)
		return err
	}

	c.consumer = nil

	return nil
}

// SubscribeTopics subscribes to a list of topics
func (c *Consumer) subscribeTopics(topics []string) error {
	return c.consumer.SubscribeTopics(topics, nil)
}

// pollEvents polls the Kafka consumer for events
func (c *Consumer) pollEvents(timeout int) kafka.Event {
	return c.consumer.Poll(timeout)
}

// CommitMessage commits a message
func (c *Consumer) commitMessage(msg *kafka.Message) {

	retry := c.config.CommitsRetry
	i := 0
	for i <= retry {
		_, err := c.consumer.CommitMessage(msg)
		if err == nil {
			log.Default().Printf("Kafka message committed with success, offset: %v, partition: %v\n", msg.TopicPartition.Offset, msg.TopicPartition.Partition)
			break
		}
		log.Default().Printf("Error committing message, offset: %v, partition: %v, retry: %v, error: %v\n", msg.TopicPartition.Offset, msg.TopicPartition.Partition, i, err)
		i++
	}
	if i > retry {
		log.Default().Printf("Failed to commit message, offset: %v, partition: %v\n", msg.TopicPartition.Offset, msg.TopicPartition.Partition)
	}
}

func (c *Consumer) isToCommit(fnResponse bool) bool {
	response := !c.config.EnableAutoCommit && fnResponse
	return response
}

func (c *Consumer) restartConsumer(ctx context.Context) {

	c.config.IsRunning = false

	if c.config.MaxRestarts == 0 {
		log.Default().Fatalln("Reached the maximum number of restarts")
	}
	c.config.MaxRestarts--

	newConsumer, err := NewConsumer(c.config)
	if err != nil {
		log.Default().Fatalf("Failed to create new consumer: %s\n", err)
	}

	err = c.Close()
	if err != nil {
		log.Default().Fatalf("Failed to Close consumer: %s\n", err)
	}

	err = newConsumer.Start(ctx)
	if err != nil {
		log.Default().Fatalf("Failed to restart consumer: %s\n", err)
	}
}

var fatalErrors []string = []string{
	"maximum poll interval",
}

func checkFatalError(err kafka.Error) bool {

	if err.IsFatal() {
		return true
	}

	for _, fatal := range fatalErrors {
		e := strings.ToUpper(err.String())
		f := strings.ToUpper(fatal)

		if strings.Contains(e, f) {
			return true
		}
	}

	return false
}

func (c *Consumer) consumeMessages(ctx context.Context, fn func(*kafka.Message) bool) {
	go func() {
		for c.config.IsRunning {
			select {
			case <-ctx.Done():
				return
			default:
				ev := c.pollEvents(c.config.PoolEventsTimeMs)
				switch e := ev.(type) {
				case *kafka.Message:

					isToCommit := fn(e)

					if c.isToCommit(isToCommit) {
						c.commitMessage(e)
					}

				case kafka.Error:
					log.Default().Printf("Error consuming message: %v\n", e)

					restart := checkFatalError(e)

					if restart {
						log.Default().Printf("A fatal error occurred, restarting consumer\n")
						c.restartConsumer(ctx)
					}
				}
			}
		}
	}()
}
