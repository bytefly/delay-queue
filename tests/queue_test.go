package tests

import (
	"context"
	"fmt"
	"testing"

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
	})
}
