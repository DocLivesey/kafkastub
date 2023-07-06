package main

import (
	"context"
	"encoding/json"
	"kafkastub/w2redis"
	"net/http"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	topic    = "stub"
	broker   = "localhost:9092"
	logLevel = logrus.InfoLevel
)

var log = logrus.New()

type KafkaInMsg struct {
	Id   int    `json:"id"`
	Body string `json:"body"`
	Done bool   `json:"done,omitempty"`
}

func Send(w http.ResponseWriter, r *http.Request) {
	kw := &kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: topic,
	}

	var msg KafkaInMsg

	err := json.NewDecoder(r.Body).Decode(&msg)

	if err != nil {
		w.WriteHeader(500)
		log.Error(err)
	}

	msg.Body += "bbb"

	err = kw.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(strconv.Itoa(msg.Id)),
			Value: []byte(msg.Body),
		},
	)

	if err != nil {
		w.WriteHeader(500)
		log.Error(err)
	}

}

func main() {

	log.SetLevel(logLevel)

	s := &http.Server{
		Addr:         ":5005",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	http.HandleFunc("/in", logger(Send))
	// http.HandleFunc("/out", logger(Out))

	// log.Fatal(http.ListenAndServe(":5001", nil))

	ctx := context.Background()

	kr := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "stub",
		MaxBytes: 1e6,
		MaxWait:  10 * time.Second,
	})

	defer kr.Close()
	rdb := w2redis.RedisClient()
	defer rdb.Close()

	go func() {
		for {
			now := time.Now()
			msg, err := kr.ReadMessage(ctx)

			if err != nil {
				log.Fatal(err)
			}

			logKafkaRedis("read from kafka", msg, time.Since(now))
			now = time.Now()

			err = w2redis.WriteMessage(ctx, msg, rdb)
			if err != nil {
				log.Fatal(err)
			}
			logKafkaRedis("write to redis", msg, time.Since(now))

		}

	}()

	log.Fatal(s.ListenAndServe())
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

func logKafkaRedis(event string, msg kafka.Message, duration time.Duration) {

	log.WithFields(logrus.Fields{
		"event":    event,
		"message":  string(msg.Key) + " " + string(msg.Value),
		"duration": duration,
	}).Info()
}
