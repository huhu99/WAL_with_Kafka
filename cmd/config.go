package main

import "github.com/Shopify/sarama"

type KafkaOption func(*sarama.Config)

// ACK options
// ACK = -1
func WithWaitForAll() KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.RequiredAcks = sarama.WaitForAll
	}
}

// ACK = 1
func WithWaitForLocal() KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.RequiredAcks = sarama.WaitForLocal
	}
}

// ACK = 0
func WithNoResponse() KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.RequiredAcks = sarama.NoResponse
	}
}

func WithMaxReqNum(num int) KafkaOption {
	return func(c *sarama.Config) {
		c.Net.MaxOpenRequests = num
	}
}

func WithFlushThreshold(num int) KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.Flush.Messages = num
	}
}

// Note that enabling idempotence cannot be used along with ACK=0/1, nor MaxOpenReq to be > 1
func WithIdempotenceEnabled() KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.Idempotent = true
		c.Producer.RequiredAcks = sarama.WaitForAll
	}
}

func WithReturnSuccess(b bool) KafkaOption {
	return func(c *sarama.Config) {
		c.Producer.Return.Successes = b
	}
}

func newKafkaConfig(opts ...KafkaOption) *sarama.Config {
	config := sarama.NewConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}
