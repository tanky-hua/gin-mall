package kafka

import (
	"context"
	"errors"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"time"
)

const (
	TopicActualConsume = "normal_consume" //正常消费topic
	TopicDeadQueue     = "dead_queue"     //死信队列
	TopicRetryQueue    = "retry_queue"    //重试队列
	TopicDelayQueue    = "delay_queue"    //延迟队列
)

var kafkaConn *kafka.Conn

func InitMQ() {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err)
	}

	// 设置发送消息的超时时间
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	fmt.Println("init error:", err)
}

// SendMessage 发送消息
// 自动创建topic，3次重试
func SendMessage(topic string, key string, msg []byte, header []kafka.Header, duration time.Duration, writer *kafka.Writer) error {

	//fmt.Println(string(msg), time.Now())

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = writer.WriteMessages(ctx,
			kafka.Message{
				Key:     []byte(key),
				Value:   msg,
				Topic:   topic,
				Time:    time.Now().Add(duration),
				Headers: header,
			},
		)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kafka.UnknownTopicOrPartition) {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		if err != nil {
			zap.S().Error("unexpected error %v", err)
			return err
		}
		break
	}
	if err != nil {
		zap.S().Error(err)
		return err
	}
	return nil
}

func Consumer(brokers []string, key, topic string, fn func(message *kafka.Message) error) (err error) {
	// 创建Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  fmt.Sprintf("%s-group-id", topic),
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})

	writer := kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks:           kafka.RequireAll,    // ack模式
		AllowAutoTopicCreation: true,                // 自动创建topic
	}
	defer writer.Close()
	// 接收消息
	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("read message error:", err)
			break
		}

		err = fn(&m)
		if err != nil {
			//fmt.Println(time.Now().In(time.Local), ":actual queue send message to retry queue", string(m.Value))
			err = SendMessage(TopicRetryQueue, string(m.Key), m.Value, m.Headers, 0, &writer)
			if err != nil {
				fmt.Println("set error2:", err)
				continue
			}
		}

		if err = r.CommitMessages(context.Background(), m); err != nil {
			zap.S().Error("failed to commit message:", err)
		}
	}

	// 程序退出前关闭Reader
	if err = r.Close(); err != nil {
		zap.S().Error("failed to close reader:", err)
	}
	return
}
