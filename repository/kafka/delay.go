package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"time"
)

// DelayMessage 延迟消息
type DelayMessage struct {
	Topic     string      `json:"topic"`      // topic
	Value     interface{} `json:"value"`      // 业务消息体
	DelayTime time.Time   `json:"delay_time"` // 延迟执行时间
	Key       string      `json:"key"`        // 消息队列的key
}

// DelayQueue 延迟队列
type DelayQueue struct {
	Topic       string        // 延迟队列名称
	DelayTime   time.Duration // 延迟时间
	Brokers     []string      // 节点请求地址
	Key         string        // 消息队列的key
	GroupID     string        // 消费组id
	FirstOffset int64         // 消息队列的offset
}

func NewDelayQueue(brokers []string, groupID string, topic string, delayTime time.Duration) *DelayQueue {
	return &DelayQueue{
		Topic:     topic,
		Brokers:   brokers,
		GroupID:   groupID,
		DelayTime: delayTime,
	}
}

// SendMessage 发送延迟消息
func (dq *DelayQueue) SendMessage(key string, value interface{}) error {
	data := DelayMessage{
		Topic:     dq.Topic,
		Value:     value,
		DelayTime: time.Now().Add(dq.DelayTime),
		Key:       key,
	}
	// 创建一个writer 向topic发送消息
	w := &kafka.Writer{
		Addr:                   kafka.TCP(dq.Brokers...),
		Balancer:               &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks:           kafka.RequireAll,    // ack模式
		Async:                  false,               // 异步
		AllowAutoTopicCreation: true,                // 自动创建topic
	}
	dataBytes, _ := json.Marshal(data)
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "retry_count", Value: []byte(fmt.Sprintf("%d", 0))})

	return SendMessage(dq.Topic, key, dataBytes, headers, dq.DelayTime, w)
}

// Consume 消费延迟队列
func (dq *DelayQueue) Consume() (err error) {
	// 创建Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  dq.Brokers,
		Topic:    dq.Topic,
		GroupID:  dq.GroupID,
		MaxBytes: 10e6, // 10MB
	})

	//// 创建一个writer 向topic发送消息
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(dq.Brokers...),
		Balancer:               &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks:           kafka.RequireAll,    // ack模式
		Async:                  false,               // 异步
		AllowAutoTopicCreation: true,                // 自动创建topic
	}
	defer writer.Close()

	// 设置消息队列的offset为当前时间的offset
	r.SetOffsetAt(context.Background(), time.Now().In(time.Local))
	//fmt.Println("set offset error:", r.SetOffsetAt(context.Background(), time.Now().In(time.Local)))
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("read message error:", err)
			break
		}
		if dq.FirstOffset == 0 {
			dq.FirstOffset = m.Offset
		}

		if m.Time.In(time.Local).Sub(time.Now()) <= 0 {
			//	发送到真实消息队列
			//fmt.Println(time.Now().In(time.Local), ":delay queue send message to actual queue", string(m.Value))
			err = SendMessage(TopicActualConsume, string(m.Key), m.Value, m.Headers, 0, writer)
			if err != nil {
				zap.S().Error("failed to commit message:", err)
				fmt.Println("set error2:", err)
				continue
			}

			// 发送成功，offset指向下一条
			dq.FirstOffset = m.Offset + 1
		} else {
			stats := r.Stats()
			if stats.Offset == m.Offset+1 { // 如果当前消息的offset+1，则说明消息已经消费，需要重新设置offset
				// 延迟时间未到，延迟消费
				time.Sleep(time.Second)
				err = r.SetOffset(dq.FirstOffset)
				if err != nil {
					zap.S().Error("failed to commit message:", err)
					continue
				}
				dq.FirstOffset = 0
			}
		}
	}

	// 程序退出前关闭Reader
	if err = r.Close(); err != nil {
		zap.S().Error("failed to close reader:", err)
	}
	return
}
