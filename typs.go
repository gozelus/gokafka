package gokafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var PartitionReachedError = fmt.Errorf(kafka.ErrPartitionEOF.String())

type Message struct {
	Topic string
	Value string
	Key   string
}

// 启动参数
type BootstrapConfig struct {
	BootstrapServers string `yaml:"BootstrapServers"`
	SecurityProtocol string `yaml:"SecurityProtocol"`
	SslCaLocation    string `yaml:"SslCaLocation"`
	SaslMechanism    string `yaml:"SaslMechanism"`
	CertPath         string `yaml:"CertPath"`
	SaslUsername     string `yaml:"SaslUsername"`
	SaslPassword     string `yaml:"SaslPassword"`
}

type ConsumerInitConfig struct {
	ApiVersionRequest      bool     `yaml:"ApiVersionRequest"`
	AutoOffsetReset        string   `yaml:"AutoOffsetReset"`
	HeartbeatIntervalMs    int      `yaml:"HeartbeatIntervalMs"`
	SessionTimeoutMs       int      `yaml:"SessionTimeoutMs"`
	MaxPollIntervalMs      int      `yaml:"MaxPollIntervalMs"`
	FetchMaxBytes          int      `yaml:"FetchMaxBytes"`
	MaxPartitionFetchBytes int      `yaml:"MaxPartitionFetchBytes"`
	EnableAutoCommit       bool     `yaml:"EnableAutoCommit"`
	GroupId                string   `yaml:"GroupId"`
	Topics                 []string `yaml:"Topics"`
}

// 初始化参数
type ProducerInitConfig struct {
	// This field indicates the number of acknowledgements
	// the leader broker must receive from ISR brokers before responding
	// to the request: 0=Broker does not send any response/ack to client
	// -1 or all=Broker will block until message is committed by all in sync replicas (ISRs).
	// If there are less than min.insync.replicas (broker configuration) in the ISR set the produce request will fail.
	Acks int `yaml:"Acks"`
	// Maximum Kafka protocol request message size.
	// Due to differing framing overhead between protocol versions the producer
	// is unable to reliably enforce a strict max message limit at produce time and
	// may exceed the maximum size by one message in protocol ProduceRequests,
	// the broker will enforce the the topic's max.message.bytes limit (see Apache Kafka documentation).
	MessageMaxBytes int `yaml:"MessageMaxBytes"`
	// Alias for message.send.max.retries: How many times to retry sending a failing Message.
	// Note: retrying may cause reordering unless enable.idempotence is set to true.
	Retries int `yaml:"Retries"`
	// The backoff time in milliseconds before retrying a protocol request.
	RetryBackoffMs int `yaml:"RetryBackoffMs"`
	// Request broker's supported API versions to adjust functionality to available protocol features.
	// If set to false, or the ApiVersionRequest fails, the fallback version broker.version.fallback will be used.
	// NOTE: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the broker.version.fallback fallback is used.
	ApiVersionRequest bool `yaml:"ApiVersionRequest"`
	// Alias for queue.buffering.max.ms: Delay in milliseconds to wait for messages
	// in the producer queue to accumulate before constructing message batches (MessageSets)
	// to transmit to brokers. A higher value allows larger and more effective (less overhead,
	// improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
	LingerMs int `yaml:"LingerMs"`
}

type Config struct {
	BootConfig         BootstrapConfig    `yaml:"BootConfig"`
	ProducerInitConfig ProducerInitConfig `yaml:"ProducerInitConfig"`
	ConsumerInitConfig ConsumerInitConfig `yaml:"ConsumerInitConfig"`
}

type Logger interface {
	Infof(str string, args ...interface{})
	InfofWithContext(ctx context.Context, str string, args ...interface{})
}
