package delayqueue

import (
	"context"
	"fmt"
	"time"
)

// 添加JobId到队列中
func (q *DelayRedisQueue) pushToReadyQueue(ctx context.Context, queueName string, jobId string) error {
	queueName = fmt.Sprint(q.name, queueName)

	err := q.client.RPush(ctx, queueName, jobId).Err()

	return err
}

// 从队列中阻塞获取JobId
func (q *DelayRedisQueue) blockPopFromReadyQueue(ctx context.Context, queues []string, timeout int) (string, error) {
	var args []string
	for _, queue := range queues {
		queue = fmt.Sprint(q.name, queue)
		args = append(args, queue)
	}
	values, err := q.client.BLPop(ctx, time.Duration(timeout)*time.Second, args...).Result()
	if err != nil {
		return "", err
	}
	if len(values) <= 0 {
		return "", nil
	}

	return values[1], nil
}
