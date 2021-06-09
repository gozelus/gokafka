package gokafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	innerProducer *kafka.Producer
}

func (p *Producer) AsyncSendMessage(msg *Message, ck func(error)) error {
	deliveryChans := make(chan kafka.Event, 10000)
	err := p.innerProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg.Value)},
		deliveryChans,
	)
	if err != nil {
		return err
	}
	go func() {
		e := <-deliveryChans
		m := e.(*kafka.Message)
		if ck != nil {
			ck(m.TopicPartition.Error)
		}
	}()
	return nil
}

func (p *Producer) SyncSendMessage(ctx context.Context, msg *Message) error {
	deliveryChans := make(chan kafka.Event, 10000)
	err := p.innerProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
		Value:          msg.Value,
	},
		deliveryChans,
	)
	if err != nil {
		return err
	}
	select {
	case e := <-deliveryChans:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func NewProducer(config Config) (*Producer, error) {
	var kafkaConf = &kafka.ConfigMap{
		"api.version.request": config.ProducerInitConfig.ApiVersionRequest,
		"message.max.bytes":   config.ProducerInitConfig.MessageMaxBytes,
		"linger.ms":           config.ProducerInitConfig.LingerMs,
		"retries":             config.ProducerInitConfig.Retries,
		"retry.backoff.ms":    config.ProducerInitConfig.RetryBackoffMs,
		"acks":                config.ProducerInitConfig.Acks,
	}
	_ = kafkaConf.SetKey("bootstrap.servers", config.BootConfig.BootstrapServers)
	switch config.BootConfig.SecurityProtocol {
	case "plaintext":
		_ = kafkaConf.SetKey("security.protocol", "plaintext")
	case "sasl_ssl":
		_ = kafkaConf.SetKey("security.protocol", "sasl_ssl")
		_ = kafkaConf.SetKey("ssl.ca.location", config.BootConfig.CertPath)
		_ = kafkaConf.SetKey("sasl.username", config.BootConfig.SaslUsername)
		_ = kafkaConf.SetKey("sasl.password", config.BootConfig.SaslPassword)
		_ = kafkaConf.SetKey("sasl.mechanism", config.BootConfig.SaslMechanism)
	default:
		return nil, kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true)
	}

	producer := Producer{}
	var err error
	if producer.innerProducer, err = kafka.NewProducer(kafkaConf); err != nil {
		return nil, err
	}
	return &producer, nil
}
