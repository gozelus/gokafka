package gokafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	innerConsumer *kafka.Consumer
	initConfig    ConsumerInitConfig
}

func NewConsumer(config Config) (*Consumer, error) {
	c := Consumer{
		initConfig: config.ConsumerInitConfig,
	}
	var err error
	kafkaconf := &kafka.ConfigMap{
		"api.version.request":       config.ConsumerInitConfig.ApiVersionRequest,
		"auto.offset.reset":         config.ConsumerInitConfig.AutoOffsetReset,
		"heartbeat.interval.ms":     config.ConsumerInitConfig.HeartbeatIntervalMs,
		"session.timeout.ms":        config.ConsumerInitConfig.SessionTimeoutMs,
		"max.poll.interval.ms":      config.ConsumerInitConfig.MaxPollIntervalMs,
		"fetch.max.bytes":           config.ConsumerInitConfig.FetchMaxBytes,
		"max.partition.fetch.bytes": config.ConsumerInitConfig.MaxPartitionFetchBytes,
		"group.id":                  config.ConsumerInitConfig.GroupId,
		"bootstrap.servers":         config.BootConfig.BootstrapServers,
	}

	switch config.BootConfig.SecurityProtocol {
	case "plaintext":
		_ = kafkaconf.SetKey("security.protocol", "plaintext")
	case "sasl_ssl":
		_ = kafkaconf.SetKey("security.protocol", "sasl_ssl")
		_ = kafkaconf.SetKey("ssl.ca.location", config.BootConfig.CertPath)
		_ = kafkaconf.SetKey("sasl.username", config.BootConfig.SaslUsername)
		_ = kafkaconf.SetKey("sasl.password", config.BootConfig.SaslPassword)
		_ = kafkaconf.SetKey("sasl.mechanism", config.BootConfig.SaslMechanism)
	case "sasl_plaintext":
		_ = kafkaconf.SetKey("security.protocol", "sasl_plaintext")
		_ = kafkaconf.SetKey("sasl.username", config.BootConfig.SaslUsername)
		_ = kafkaconf.SetKey("sasl.password", config.BootConfig.SaslPassword)
		_ = kafkaconf.SetKey("sasl.mechanism", config.BootConfig.SaslMechanism)
	}

	if c.innerConsumer, err = kafka.NewConsumer(kafkaconf); err != nil {
		return nil, err
	}
	if err := c.innerConsumer.SubscribeTopics(config.ConsumerInitConfig.Topics, nil); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Consumer) PollWithAsyncCommit(ctx context.Context, commit func() error) error {
	return nil
}
func (c *Consumer) PollWithSyncCommit(handle func(message *Message) error) error {
	if c.initConfig.EnableAutoCommit {
		return fmt.Errorf("consumer auto commit is true")
	}
	msg, err := c.innerConsumer.ReadMessage(-1)
	if err != nil {
		return err
	}
	if err := handle(&Message{
		Value: msg.Value,
		Key:   string(msg.Key),
	}); err != nil {
		return err
	}
	_, err = c.innerConsumer.CommitMessage(msg)
	if err != nil {
		return err
	}
	return nil
}
func (c *Consumer) PollWithAutoCommit() (*Message, error) {
	if !c.initConfig.EnableAutoCommit {
		return nil, fmt.Errorf("consumer auto commit is false")
	}
	ev, err := c.innerConsumer.ReadMessage(-1)
	if err != nil {
		return nil, err
	}
	return &Message{
		Value: ev.Value,
		Key:   string(ev.Key),
	}, nil
}
