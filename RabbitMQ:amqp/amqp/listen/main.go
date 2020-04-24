package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RpcMessage struct {
	Event   string `json:"event"`
	Payload []byte `json:"payload"`
}

func isAboutPromotionEvent(channel *amqp.Channel, channelName string, message []byte) {
	rpcMessage := &RpcMessage{}
	_ = json.Unmarshal(message, rpcMessage)
	log.Printf(channelName+" Event: %s Payload: %s", rpcMessage.Event, rpcMessage.Payload)

	/*message, _ = json.Marshal(rpcMessage)

	switch rpcMessage.Event {
	case "GET_SYSTEM_META":
		rpcChannelSent(channel, channelName, message)
	case "GET_PAYMENT_METHODS":
		rpcChannelSent(channel, channelName, message)
	case "GET_STORE_ADVERTISEMENTS":
		rpcChannelSent(channel, channelName, message)
	default:
		rpcChannelSent(channel, channelName, message)
	}
	*/
}

func rpcChannelSent(channel *amqp.Channel, channelName string, message []byte) {
	switch channelName {
	case "rpc_queue":
		channelName = "second_rpc_queue"
	case "stomp.zakkaya.rpc.gui":
		channelName = "second_stomp.zakkaya.rpc.gui"
	}
	args := amqp.Table{}
	args["x-message-ttl"] = int32(5000)
	q, err := channel.QueueDeclare(
		channelName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		args,        // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = channel.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
	failOnError(err, "Failed to publish a message")
}

func rpcChannelListening(channel *amqp.Channel, channelName string) {
	args := amqp.Table{}
	args["x-message-ttl"] = int32(5000)
	q, err := channel.QueueDeclare(
		channelName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		args,        // arguments
	)
	failOnError(err, "Failed to declare a queue")
	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	go func() {
		for d := range msgs {
			isAboutPromotionEvent(channel, channelName, d.Body)
		}
	}()
}

func main() {
	//conn, err := amqp.Dial("amqp://rabbit1:password123@10.255.254.119:5672/")
	conn, err := amqp.Dial("amqp://rabbit1:password123@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	forever := make(chan bool)
	rpcChannelListening(ch, "rpc_queue")
	//rpcChannelListening(ch, "second_rpc_queue")
	rpcChannelListening(ch, "stomp.zakkaya.rpc.gui")
	//rpcChannelListening(ch, "second_stomp.zakkaya.rpc.gui")
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
