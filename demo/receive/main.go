package main

import (
	"encoding/json"
	"fmt"
	"game-lottery/psmq"
	"log"
	"sync"
)

type message struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func main() {
	pb, err := psmq.New("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer pb.Close()

	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			s, err := psmq.NewSubscriber(pb, "master", 30, printMsg)
			if err != nil {
				log.Fatal(err)
				return
			}

			s.Run()
		}(wg, i)
	}
	wg.Wait()
}

func printMsg(data []byte) {
	var msg message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return
	}
	fmt.Printf("Receive task \"%s\", status \"%s\"\n", msg.Name, msg.Status)
	return
}
