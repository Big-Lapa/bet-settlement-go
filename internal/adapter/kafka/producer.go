// Package kafka contains the Kafka adapters (segmentio/kafka-go).
package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
)

// Producer publishes outbox messages. It implements outbox.Publisher.
type Producer struct {
	w *kafkago.Writer
}

// NewProducer creates a producer. Topic is taken from each message.
func NewProducer(brokers []string) *Producer {
	return &Producer{w: &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Balancer:     &kafkago.Hash{}, // same key -> same partition -> ordered per player
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		// Created by docker-compose / infra in real setups; handy for local runs.
		AllowAutoTopicCreation: true,
	}}
}

// Publish sends a batch synchronously and returns after all acks.
func (p *Producer) Publish(ctx context.Context, msgs []app.OutboxMessage) error {
	km := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		km[i] = kafkago.Message{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload}
	}
	if err := p.w.WriteMessages(ctx, km...); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	return nil
}

// Close flushes and closes the writer.
func (p *Producer) Close() error { return p.w.Close() }
