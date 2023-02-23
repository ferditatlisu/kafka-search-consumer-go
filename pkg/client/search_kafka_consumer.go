package client

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka-search-consumer-go/pkg/config"
	"strings"
	"time"
)


type SearchKafkaConsumer interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Stop()
}

type searchKafkaConsumer struct {
	consumer *kafka.Reader
}

func NewSearchKafkaConsumer(kafkaConfig config.SearchKafkaConfig) *searchKafkaConsumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:        strings.Split(kafkaConfig.Servers, ","),
		GroupID:        kafkaConfig.Group,
		Topic:          kafkaConfig.Topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // flushes commits to Kafka every second
	}

	newConsumer := kafka.NewReader(readerConfig)
	return &searchKafkaConsumer{newConsumer}
}

func (k *searchKafkaConsumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := k.consumer.ReadMessage(ctx)
	if err != nil {
		return msg, err
	}

	err = k.consumer.CommitMessages(ctx, msg)
	if err != nil {
		return msg, err
	}

	return msg, nil
}

func (k *searchKafkaConsumer) Stop() {
	k.consumer.Close()
}
