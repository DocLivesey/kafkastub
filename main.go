package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	topic    = "stub2"
	broker   = "localhost:9092"
	logLevel = logrus.InfoLevel
)

var log = logrus.New()

type KafkaMsg struct {
	Id   int    `json:"id"`
	Body string `json:"body"`
	Done bool   `json:"done,omitempty"`
}

func (p *MsgProducer) Send(w http.ResponseWriter, r *http.Request) {
	var msg KafkaMsg

	err := json.NewDecoder(r.Body).Decode(&msg)

	if err != nil {
		w.WriteHeader(500)
		log.Error(err)
	}

	msg.Body += "bbb"

	err = p.ProduceMsg(msg)

	if err != nil {
		w.WriteHeader(500)
		log.Error(err)
	}
}

func main() {

	log.SetLevel(logLevel)

	msg := KafkaMsg{
		Id:   15,
		Body: "aaaa",
	}

	producer, err := NewProducer(broker)
	if err != nil {
		log.Fatalf("failed to init kafka producer -- %s ", err)
	}

	p := NewMsgProducer(producer, topic)

	s := &http.Server{
		Addr:         ":5005",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	http.HandleFunc("/in", logger(p.Send))

	p.ProduceMsg(msg)

	ctx := context.Background()

	rdb := RedisClient()
	defer rdb.Close()

	consumer, err := NewConsumer(broker)
	if err != nil {
		log.Fatalf("failed to init kafka consumer -- %s", err)
	}

	c := NewMsgConsumer(consumer, topic)

	go func() {
		c.ConsumeAndWrite(rdb, ctx)
	}()

	defer c.consumer.Close()

	err = s.ListenAndServe()
	if err != nil {
		log.Fatalf("failed to init server %s", err.Error())
	}
}

func logger(handler http.HandlerFunc) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		handler.ServeHTTP(w, r)
		duration := time.Since(now)

		log.WithFields(logrus.Fields{
			"request":  r.RequestURI,
			"duration": duration,
		}).Info()

	}

}

// func logKafkaRedis(event string, msg kafka.Message, duration time.Duration) {

// 	log.WithFields(logrus.Fields{
// 		"event":    event,
// 		"message":  string(msg.Key) + " " + string(msg.Value),
// 		"duration": duration,
// 	}).Info()
// }
