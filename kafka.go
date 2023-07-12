package main

import (
	"context"
	"encoding/json"

	// "log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type MsgProducer struct {
	producer   *kafka.Producer
	topic      string
	deliverych chan kafka.Event
}

type MsgConsumer struct {
	consumer *kafka.Consumer
	topic    string
}

func NewProducer(broker string) (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"client.id":         "stub",
		"acks":              "all",
	})
}

func NewMsgProducer(p *kafka.Producer, topic string) *MsgProducer {
	return &MsgProducer{
		producer:   p,
		topic:      topic,
		deliverych: make(chan kafka.Event, 100),
	}
}

func (mp *MsgProducer) ProduceMsg(msg KafkaMsg) error {

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = mp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &mp.topic, Partition: kafka.PartitionAny},
		Value:          payload,
	},
		mp.deliverych)
	if err != nil {
		return err
	}

	<-mp.deliverych
	return nil
}

func NewConsumer(broker string) (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "stub_consumer",
	})
}

func NewMsgConsumer(c *kafka.Consumer, topic string) *MsgConsumer {
	return &MsgConsumer{
		consumer: c,
		topic:    topic,
	}
}

func (mc *MsgConsumer) ConsumeAndWrite(rdb *redis.Client, ctx context.Context) {
	err := mc.consumer.Subscribe(mc.topic, nil)

	if err != nil {
		log.Fatalf("failed to consume %s \n", err)
	}

	for {
		ev := mc.consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			var msg KafkaMsg
			_ = json.Unmarshal(e.Value, &msg)
			WriteMessage(ctx, msg, rdb)
		case *kafka.Error:
			log.WithFields(logrus.Fields{
				"message": "kafka sent error",
				"err":     err,
			}).Error()
		}
	}
}
