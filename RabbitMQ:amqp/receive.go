package main

import (
	"log"
	//"encoding/base64"
	"encoding/json"
	"github.com/streadway/amqp"
	"strconv"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RpcMessage struct {
	PromotionName	string		`json:"promotion_name"`
	Enable			bool		`json:"enable"`
	Action			string		`json:"action"`
	ProductTag		string		`json:"product_tag"`
	AisleNo			string		`json:"aisle_no"`
	PaymentModule	string		`json:"payment_module"`
	Discount		string		`json:"discount"`
}

func main() {
	conn, err := amqp.Dial("amqp://rabbit1:password123@10.255.254.122:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc.promotion.server", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
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

	RpcMessage := &RpcMessage{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			log.Print("-------------------------------------------------- DECODE --------------------------------------------------")
			_ = json.Unmarshal(d.Body, RpcMessage)
			log.Print("PromotionName: " + RpcMessage.PromotionName)
			log.Print("Enable: " + strconv.FormatBool(RpcMessage.Enable))
			log.Print("Action: " + RpcMessage.Action)
			log.Print("ProductTag: " + RpcMessage.ProductTag)
			log.Print("AisleNo: " + RpcMessage.AisleNo)
			log.Print("PaymentModule: " + RpcMessage.PaymentModule)
			log.Print("Discount: " + RpcMessage.Discount)
			log.Print("-------------------------------------------------- DECODE --------------------------------------------------")
		}
	}()

	//decoded, err := base64.StdEncoding.DecodeString(d.Body.payload)
	//if err != nil {
	//	log.Print("decode error:", err)
	//	return
	//}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}