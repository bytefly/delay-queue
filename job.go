package delayqueue

import (
	"context"

	"github.com/vmihailenco/msgpack"
)

// Job 使用msgpack序列化后保存到Redis,减少内存占用
type Job struct {
	Topic string `json:"topic" msgpack:"1"`
	Id    string `json:"id" msgpack:"2"`    // job唯一标识ID
	Delay int64  `json:"delay" msgpack:"3"` // 延迟时间, unix时间戳
	TTR   int64  `json:"ttr" msgpack:"4"`
	Body  string `json:"body" msgpack:"5"`
}

// 获取Job
func (q *DelayRedisQueue) getJob(ctx context.Context, key string) (*Job, error) {
	value, err := q.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, nil
	}

	job := &Job{}
	err = msgpack.Unmarshal([]byte(value), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// 添加Job
func (q *DelayRedisQueue) putJob(ctx context.Context, key string, job Job) error {
	value, err := msgpack.Marshal(job)
	if err != nil {
		return err
	}
	err = q.client.Set(ctx, key, value, 0).Err()

	return err
}

// 删除Job
func (q *DelayRedisQueue) removeJob(ctx context.Context, key string) error {
	err := q.client.Del(ctx, key).Err()

	return err
}
