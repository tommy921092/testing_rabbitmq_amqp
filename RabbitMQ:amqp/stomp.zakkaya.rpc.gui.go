package main

import (
	"log"
	//"encoding/base64"
	"encoding/json"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RpcMessage struct {
	Event   string		`json:"event"`
	Payload []byte      `json:"payload"`
}

//func decodeBase64(encodeMessage []byte) string {
//	decoded, _ := base64.StdEncoding.DecodeString(string(encodeMessage))
//	return string(decoded)
//}

func sendMessage(message []byte) {
	
	//conn, err := amqp.Dial("amqp://rabbit1:password123@10.255.254.138:5672/")
	conn, err := amqp.Dial("amqp://rabbit1:password123@localhost:5672/")

	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	args := amqp.Table{}
	args["x-message-ttl"] = int32(5000)

	q, err := ch.QueueDeclare(
		"stomp.zakkaya.rpc.gui", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		args,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Publish(
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

func main() {
	//conn, err := amqp.Dial("amqp://rabbit1:password123@10.255.254.138:5672/")
	conn, err := amqp.Dial("amqp://rabbit1:password123@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	args := amqp.Table{}
	args["x-message-ttl"] = int32(5000)

	q, err := ch.QueueDeclare(
		"stomp.zakkaya.rpc.gui", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		args,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	rpcMessage := &RpcMessage{}

	go func() {
		for d := range msgs {
			log.Print("-------------------------------------------------- START --------------------------------------------------")
			log.Printf("GET: %s", d.Body)
			log.Print("-------------------------------------------------- DECODE --------------------------------------------------")
			_ = json.Unmarshal(d.Body, rpcMessage)
			log.Printf("%s", rpcMessage.Event)
			log.Printf("%s", rpcMessage.Payload)
			log.Print("-------------------------------------------------- SEND --------------------------------------------------")
			sendMessage(d.Body)
			log.Printf("SEND: %s", d.Body)
			log.Print("-------------------------------------------------- END --------------------------------------------------")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}