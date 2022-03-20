package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	delayqueue "github.com/go-online-public/delay-queue"
	"github.com/go-redis/redis/v8"
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

func TestRedisQuere(t *testing.T) {
	convey.Convey("TestRedisQuere", t, func() {
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
					TTR:   1,
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
		convey.Convey("msg exceed TTR", func() {
			ctx := context.Background()
			queueName := "test_exceed_ttr"
			topicName := "ttr"
			ttr := int64(1)

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

			// 2s后消息如果还能收到，则出现问题
			time.Sleep(time.Second * time.Duration(ttr+1))
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
				assert.Fail(t, "ttr fail")
			case err := <-errChan:
				assert.Nil(t, err)
				assert.Fail(t, "err before ttr")
			default:
			}
		})
		convey.Convey("delay msg", func() {
			ctx := context.Background()
			queueName := "test_delay"
			topicName := "delay"
			delayTime := int64(2)

			// 初始化消息队列
			db := getMiniRedisClient(t)
			queue := delayqueue.New(ctx, queueName, 1, db)

			// 发送延迟消息
			id := fmt.Sprint(0)
			convey.So(queue.Push(ctx, delayqueue.Job{
				Topic: topicName,
				Id:    id,
				Delay: delayTime,
				TTR:   int64(time.Hour),
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
			default:
			}

			// 3s后还没收到消息，则说明delay失败
			time.Sleep(time.Duration(delayTime+1) * time.Second)
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
