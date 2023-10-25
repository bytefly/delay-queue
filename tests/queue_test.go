package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	delayqueue "github.com/bytefly/delay-queue"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func getBodyWithID(id string) string {
	return fmt.Sprintf("body+%v", id)
}

func getMiniRedisClient(t *testing.T) *redis.Client {
	s := miniredis.RunT(t)
	db := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	return db
}

func TestInstantMsg(t *testing.T) {
	convey.Convey("TestInstantMsg", t, func() {
		convey.Convey("instant msg", func() {
			ctx := context.Background()
			queueName := "test_instant"
			topicName := "instant"
			testCnt := 10

			// 初始化消息队列
			db := getMiniRedisClient(t)
			queue := delayqueue.New(ctx, queueName, 1, db)

			// 发送实时消息
			for i := 0; i < testCnt; i++ {
				id := fmt.Sprint(i)
				convey.So(queue.Push(ctx, delayqueue.Job{
					Topic: topicName,
					Id:    id,
					Delay: 0,
					TTR:   600, // 测试期间不会重试
					Body:  getBodyWithID(id),
				}), convey.ShouldBeNil)
			}

			// 校验消息是否被收到
			for i := 0; i < testCnt; i++ {
				job, err := queue.Pop(ctx, []string{topicName})
				assert.Nil(t, err)
				assert.NotNil(t, job)
				assert.Equal(t, getBodyWithID(job.Id), job.Body)
			}
		})
	})
}
func TestTTR(t *testing.T) {
	convey.Convey("TestTTR", t, func() {
		convey.Convey("not retry before ttr", func() {
			ctx := context.Background()
			queueName := "test_ttr"
			topicName := "ttr"
			ttr := int64(10) // 10秒后重试

			// 初始化消息队列
			db := getMiniRedisClient(t)
			queue := delayqueue.New(ctx, queueName, 1, db)

			// 发送实时消息
			id := fmt.Sprint(0)
			convey.So(queue.Push(ctx, delayqueue.Job{
				Topic: topicName,
				Id:    id,
				Delay: 0,
				TTR:   ttr,
				Body:  getBodyWithID(id),
			}), convey.ShouldBeNil)

			finishChan := make(chan struct{})
			errChan := make(chan error)
			// 第一次消费：不删除，保证重试
			_, err := queue.Pop(ctx, []string{topicName})
			assert.Nil(t, err)
			// ttr时间内若已经重试，则说明ttr未生效
			go func() {
				_, err := queue.Pop(ctx, []string{topicName})
				if err != nil {
					errChan <- err
				}
				finishChan <- struct{}{}
			}()
			select {
			case <-time.NewTimer(time.Second * 2).C: // 等待2秒
			case <-finishChan:
				assert.Fail(t, "retry before ttr")
			}
			// ttr后如果未收到，则说明未重试
			time.Sleep(time.Second * time.Duration(ttr+1))
			go func() {
				_, err := queue.Pop(ctx, []string{topicName})
				if err != nil {
					errChan <- err
				}
				finishChan <- struct{}{}
			}()

			select {
			case <-finishChan:
			case <-errChan:
				assert.Fail(t, "err before ttr")
			default:
				assert.Fail(t, "not retry after ttr")
			}
		})
	})
}

func TestDelayMsg(t *testing.T) {
	convey.Convey("TestDelayMsg", t, func() {
		convey.Convey("delay msg", func() {
			ctx := context.Background()
			queueName := "test_delay"
			topicName := "delay"
			delayTime := int64(5) // 5s后消费

			// 初始化消息队列
			db := getMiniRedisClient(t)
			queue := delayqueue.New(ctx, queueName, 1, db)

			// 发送延迟消息
			id := fmt.Sprint(0)
			convey.So(queue.Push(ctx, delayqueue.Job{
				Topic: topicName,
				Id:    id,
				Delay: delayTime,
				TTR:   int64(time.Hour), // 测试期间不重试
				Body:  getBodyWithID(id),
			}), convey.ShouldBeNil)

			// 立刻收到消息，则说明delay失败
			finishChan := make(chan struct{})
			errChan := make(chan error)
			go func() {
				_, err := queue.Pop(ctx, []string{topicName})
				if err != nil {
					errChan <- err
				}
				finishChan <- struct{}{}
			}()

			select {
			case <-finishChan:
				assert.Fail(t, "delay fail")
			case err := <-errChan:
				assert.Nil(t, err)
				assert.Fail(t, "err before get delay msg")
			case <-time.NewTimer(time.Second * 2).C: // 等待2s
			}

			// 延迟时间后还没收到消息，则说明delay失败
			time.Sleep(time.Duration(delayTime+2) * time.Second)
			select {
			case <-finishChan:
			case err := <-errChan:
				assert.Nil(t, err)
				assert.Fail(t, "err when should get delay msg")
			default:
				assert.Fail(t, "not get delay msg")
			}
		})
	})
}
