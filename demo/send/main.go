package main

import (
	"encoding/json"
	"game-lottery/pbmq"
	"log"
)

type message struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func main() {
	pb, err := pbmq.New("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer pb.Close()

	p, err := pbmq.NewPublisher(pb, "application/json", "master")
	if err != nil {
		log.Fatal(err)
		return
	}

	msg := message{"test", "success"}
	data, _ := json.Marshal(&msg)

	p.Publish(data)
}
