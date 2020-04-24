package main

import (
	"log"
	"encoding/json"
	"github.com/streadway/amqp"
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

func sendMessage(message RpcMessage) {
	
	conn, err := amqp.Dial("amqp://rabbit1:password123@10.255.254.122:5672/")
	//conn, err := amqp.Dial("amqp://rabbit1:password123@localhost:5672/")

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

	data, err := json.Marshal(message)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		})

	failOnError(err, "Failed to publish a message")
}

func main() {
	// Example Message
	message := RpcMessage{PromotionName: "GOOGLE_CALENDAR", Enable: true, Action: "DISCOUNT", ProductTag: "ALL", AisleNo: "ALL", PaymentModule: "ALL", Discount: "1.00"}
	// message := RpcMessage{PromotionName: "QRCODE_DISCOUNT", Enable: true, Action: "DISCOUNT", ProductTag: "ALL", AisleNo: "ALL", PaymentModule: "ALL", Discount: "1.00"}
	// message := RpcMessage{PromotionName: "QRCODE_REDEEM_PRODUCT", Enable: true, Action: "VENDOUT", ProductTag: "ALL", AisleNo: "1", PaymentModule: "ALL", Discount: "NULL"}
	sendMessage(message)
}
