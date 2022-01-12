package delayqueue

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// BucketItem bucket中的元素
type BucketItem struct {
	timestamp int64
	jobId     string
}

// 添加JobId到bucket中
func (q *DelayRedisQueue) pushToBucket(ctx context.Context, key string, timestamp int64, jobId string) error {
	err := q.client.ZAdd(ctx, key, &redis.Z{
		Score:  float64(timestamp),
		Member: jobId,
	}).Err()

	return err
}

// 从bucket中获取延迟时间最小的JobId
func (q *DelayRedisQueue) getFromBucket(ctx context.Context, key string) (*BucketItem, error) {
	values, err := q.client.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}

	item := &BucketItem{}
	item.timestamp = int64(values[0].Score)
	item.jobId = string(values[0].Member.([]byte))

	return item, nil
}

// 从bucket中删除JobId
func (q *DelayRedisQueue) removeFromBucket(ctx context.Context, bucket string, jobId string) error {
	err := q.client.ZRem(ctx, bucket, jobId).Err()

	return err
}
