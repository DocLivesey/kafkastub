package main

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func RedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func WriteMessage(ctx context.Context, msg KafkaMsg, rdb *redis.Client) error {

	err := rdb.Set(ctx, strconv.Itoa(msg.Id), string(msg.Body), 0).Err()
	if err != nil {
		return err
	}

	return nil
}
