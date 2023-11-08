package main

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    fmt.Println("Hello World");
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672");

    defer conn.Close();

    if (err != nil) {
        fmt.Println("Could not connect to the server");
        return;
    }

    ch, err := conn.Channel();
    if (err != nil) {
        fmt.Println("Could not get the channel");
    }
    defer ch.Close();

    q, err := ch.QueueDeclare(
        "Test",
        false,
        false,
        false,
        false,
        nil,
    );

    if (err != nil) {
        fmt.Println("Not able to get the queue");
    }

    if os.Args[1] == "r" {
        fmt.Println("Receiving msg")
        receive(ch, &q)
    } else {
        fmt.Println("Sending msg")
        i := 1;
        for {
            send(ch, &q, i)
            fmt.Printf("Sending msg: %d\n", i)
            i++
            time.Sleep(time.Second);
        }
    }
}

func receive(ch *amqp.Channel, q *amqp.Queue) {
    msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil);
    if (err != nil) {
        fmt.Println("Unable to consume")
        fmt.Println(err)
    }

    for msg := range msgs {
        fmt.Printf("Received body %s \n", msg.Body)
    }
}


func send(ch *amqp.Channel, q *amqp.Queue, i int) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second);

    defer cancel()

    ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
        ContentType: "text/plain",
        Body: []byte(fmt.Sprintf("Hello from client %d", i)),
    });

}
