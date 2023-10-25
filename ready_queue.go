package delayqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// 添加JobId到队列中
func (q *DelayRedisQueue) pushToReadyQueue(ctx context.Context, topic string, jobId string) error {
	topic = fmt.Sprint(q.name, topic)

	err := q.client.RPush(ctx, topic, jobId).Err()

	return err
}

// 从队列中阻塞获取JobId
func (q *DelayRedisQueue) blockPopFromReadyQueue(ctx context.Context, queues []string, timeout time.Duration) (string, error) {
	var args []string
	for _, queue := range queues {
		queue = fmt.Sprint(q.name, queue)
		args = append(args, queue)
	}
	values, err := q.client.BLPop(ctx, timeout, args...).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(values) <= 0 {
		return "", nil
	}

	return values[1], nil
}
