package main

import (
	"encoding/json"
	"game-lottery/psmq"
	"log"
)

type message struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func main() {
	ps, err := psmq.New("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer ps.Close()

	p, err := ps.NewPublisher("application/json", "master")
	if err != nil {
		log.Fatal(err)
		return
	}

	msg := message{"test", "success"}
	data, _ := json.Marshal(&msg)

	p.Publish(data)
}
