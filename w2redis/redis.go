package w2redis

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func RedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func WriteMessage(ctx context.Context, msg kafka.Message, rdb *redis.Client) error {

	err := rdb.Set(ctx, string(msg.Key), string(msg.Value), 0).Err()
	if err != nil {
		return err
	}

	return nil
}
