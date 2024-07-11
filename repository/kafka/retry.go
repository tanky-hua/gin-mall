package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"strconv"
	"time"
)

type RetryMessage struct {
	Key           string        `json:"key"`
	Topic         string        `json:"topic"`
	Value         interface{}   `json:"value"`
	RetryTime     time.Time     `json:"retry_time"`
	RetryCount    int           `json:"retry_count"`
	RetryMaxCount int           `json:"retry_max_count"`
	RetryInterval time.Duration `json:"retry_interval"`
}

type RetryQueue struct {
	Brokers       []string
	GroupID       string
	Key           string
	Topic         string
	RetryMaxCount int
	RetryInterval time.Duration `json:"retry_interval"`
	Fn            func(message *kafka.Message) error
}

func NewRetryQueue(brokers []string, groupID string, topic string, retryMaxCount int, retryInterval time.Duration, fn func(message *kafka.Message) error) *RetryQueue {
	if retryMaxCount <= 0 { //默认3次重试发送
		retryMaxCount = 3
	}
	if retryInterval <= 0 { //默认10秒间隔
		retryInterval = 10 * time.Second
	}
	if groupID == "" {
		groupID = "retry_queue_group"
	}
	return &RetryQueue{
		Brokers:       brokers,
		GroupID:       groupID,
		Topic:         topic,
		RetryMaxCount: retryMaxCount,
		RetryInterval: retryInterval,
		Fn:            fn,
	}
}

func (c *RetryQueue) Consume() (err error) {
	// 创建Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  c.Brokers,
		Topic:    c.Topic,
		GroupID:  c.GroupID,
		MaxBytes: 10e6, // 10MB
	})

	writer := kafka.Writer{
		Addr:                   kafka.TCP(c.Brokers...),
		Balancer:               &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks:           kafka.RequireAll,    // ack模式
		AllowAutoTopicCreation: true,                // 自动创建topic
	}
	defer writer.Close()
	for {
		m, err := reader.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("read message error:", err)
			break
		}

		err = c.retryFailedMessages(context.Background(), TopicRetryQueue, []kafka.Message{m}, c.RetryInterval, c.RetryMaxCount, &writer)
		if err != nil {
			fmt.Println("failed to retry failed messages:", err)
			continue
		}
		if err = reader.CommitMessages(context.Background(), m); err != nil {
			zap.S().Error("failed to commit message:", err)
			fmt.Println("failed to commit message:", err)
		}
	}
	// 程序退出前关闭Reader
	if err = reader.Close(); err != nil {
		zap.S().Error("failed to close reader:", err)
	}
	return
}

func getRetryCountFromMessageHeader(msg kafka.Message) int {
	for _, header := range msg.Headers {
		if header.Key == "retry_count" {
			retryCount, err := strconv.Atoi(string(header.Value))
			if err == nil {
				return retryCount
			}
		}
	}
	return 0
}

func (c *RetryQueue) retryFailedMessages(ctx context.Context, topic string, failedMessages []kafka.Message, retryDelay time.Duration, maxRetries int, writer *kafka.Writer) error {
	for _, msg := range failedMessages {
		// 检查消息的重试次数
		retryCount := getRetryCountFromMessageHeader(msg)
		if retryCount >= maxRetries-1 {
			// 超过最大重试次数，记录错误日志或采取其他措施
			zap.S().Info("kafka message add to dead queue", string(msg.Value))

			err := SendMessage(TopicDeadQueue, string(msg.Key), msg.Value, nil, 0, writer)
			if err != nil {
				zap.S().Error("failed to send message to dead queue:", err)
			}
			continue
		}

		err := c.Fn(&msg)
		if err != nil {
			// 记录错误日志或采取其他措施
			// 增加重试次数并更新消息头
			var headers []kafka.Header
			headers = append(headers, kafka.Header{Key: "retry_count", Value: []byte(fmt.Sprintf("%d", retryCount+1))})

			// 等待重试延迟
			time.Sleep(retryDelay)
			err = SendMessage(topic, string(msg.Key), msg.Value, headers, 0, writer)
			if err != nil {
				// 记录错误日志或采取其他措施
				fmt.Printf("Error while retrying message %s: %v\n", msg.Key, err)
			} else {
				fmt.Printf("Message %s retried successfully\n", msg.Key)
			}
		}
	}

	return nil
}
