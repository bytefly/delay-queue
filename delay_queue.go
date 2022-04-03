package delayqueue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	DefaultQueueBlockTimeout = 10 * time.Second
)

var (
	// 每个定时器对应一个bucket
	timers []*time.Ticker
)

type DelayRedisQueue struct {
	client *redis.Client

	name      string
	bucketCnt int

	bucketNameChan <-chan string
}

func New(ctx context.Context, name string, bucketCnt int, redisClient *redis.Client) *DelayRedisQueue {
	q := &DelayRedisQueue{
		name:      name,
		bucketCnt: bucketCnt,
		client:    redisClient,
	}

	q.initTimers(ctx)
	q.bucketNameChan = q.generateBucketName()

	return q
}

// Push 添加一个Job到队列中
func (q *DelayRedisQueue) Push(ctx context.Context, job Job) error {
	if job.Id == "" || job.Topic == "" || job.Delay < 0 || job.TTR <= 0 {
		return errors.New("invalid job")
	}

	err := q.putJob(ctx, job.Id, job)
	if err != nil {
		log.Printf("添加job到job pool失败#job-%+v#%s", job, err.Error())
		return err
	}
	delayTime := time.Now().Add(time.Duration(job.Delay) * time.Second)
	err = q.pushToBucket(ctx, <-q.bucketNameChan, int64(delayTime.Second()), job.Id)
	if err != nil {
		log.Printf("添加job到bucket失败#job-%+v#%s", job, err.Error())
		return err
	}

	return nil
}

// Pop 轮询获取Job
func (q *DelayRedisQueue) Pop(ctx context.Context, topics []string) (*Job, error) {
	jobId, err := q.blockPopFromReadyQueue(ctx, topics, DefaultQueueBlockTimeout)
	if err != nil {
		return nil, err
	}

	// 队列为空
	if jobId == "" {
		return nil, nil
	}

	// 获取job元信息
	job, err := q.getJob(ctx, jobId)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}

	timestamp := time.Now().Unix() + job.TTR
	err = q.pushToBucket(ctx, <-q.bucketNameChan, timestamp, job.Id)

	return job, err
}

// Remove 删除Job
func (q *DelayRedisQueue) Remove(ctx context.Context, jobId string) error {
	return q.removeJob(ctx, jobId)
}

// Get 查询Job
func (q *DelayRedisQueue) Get(ctx context.Context, jobId string) (*Job, error) {
	job, err := q.getJob(ctx, jobId)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}
	return job, err
}

// 轮询获取bucket名称, 使job分布到不同bucket中, 提高扫描速度
func (q *DelayRedisQueue) generateBucketName() <-chan string {
	c := make(chan string)
	go func() {
		i := 1
		for {
			c <- fmt.Sprint(q.name, i)
			if i >= q.bucketCnt {
				i = 1
			} else {
				i++
			}
		}
	}()

	return c
}

// 初始化定时器
func (q *DelayRedisQueue) initTimers(ctx context.Context) {
	timers = make([]*time.Ticker, q.bucketCnt)
	var bucketName string
	for i := 0; i < q.bucketCnt; i++ {
		timers[i] = time.NewTicker(1 * time.Second)
		bucketName = fmt.Sprint(q.name, i+1)
		go q.waitTicker(ctx, timers[i], bucketName)
	}
}

func (q *DelayRedisQueue) waitTicker(ctx context.Context, timer *time.Ticker, bucketName string) {
	for t := range timer.C {
		q.tickHandler(ctx, t, bucketName)
	}
}

// 扫描bucket, 取出延迟时间小于当前时间的Job
func (q *DelayRedisQueue) tickHandler(ctx context.Context, t time.Time, bucketName string) {
	for {
		bucketItem, err := q.getFromBucket(ctx, bucketName)
		if err != nil {
			log.Printf("扫描bucket错误#bucket-%s#%s", bucketName, err.Error())
			continue
		}

		// 集合为空
		if bucketItem == nil {
			continue
		}

		// 延迟时间未到
		currTime := t.Unix()
		if bucketItem.timestamp > currTime {
			continue
		}

		// 延迟时间小于等于当前时间, 取出Job元信息并放入ready queue
		job, err := q.getJob(ctx, bucketItem.jobId)
		if err != nil {
			log.Printf("获取Job元信息失败#bucket-%s#%s", bucketName, err.Error())
			continue
		}

		// job元信息不存在, 从bucket中删除
		if job == nil {
			q.removeFromBucket(ctx, bucketName, bucketItem.jobId)
			continue
		}

		// 再次确认元信息中delay是否小于等于当前时间
		if job.Delay > t.Unix() {
			// 从bucket中删除旧的jobId
			q.removeFromBucket(ctx, bucketName, bucketItem.jobId)
			// 重新计算delay时间并放入bucket中
			q.pushToBucket(ctx, <-q.bucketNameChan, job.Delay, bucketItem.jobId)
			continue
		}

		err = q.pushToReadyQueue(ctx, job.Topic, bucketItem.jobId)
		if err != nil {
			log.Printf("JobId放入ready queue失败#bucket-%s#job-%+v#%s",
				bucketName, job, err.Error())
			continue
		}

		// 从bucket中删除
		q.removeFromBucket(ctx, bucketName, bucketItem.jobId)
	}
}
