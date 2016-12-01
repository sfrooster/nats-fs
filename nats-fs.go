package main

import (
	//"encoding/json"
	"fmt"
	"os"

	//"github.com/davecgh/go-spew/spew"

	"github.com/nats-io/go-nats-streaming/pb"
	stanfstore "github.com/nats-io/nats-streaming-server/stores"
)

var usageStr = `
Usage: nats-fs <file> <topic>
`

func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		usage()
	}

	fstorelimits := stanfstore.DefaultStoreLimits
	fstorelimits.MsgStoreLimits.MaxAge = 0
	fstorelimits.MsgStoreLimits.MaxBytes = 0
	fstorelimits.MsgStoreLimits.MaxMsgs = 0

	fstore, _, err := stanfstore.NewFileStore(args[0], &fstorelimits)

	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}

	channel := fstore.LookupChannel(args[1])

	count := 0
	min := uint64(1)
	max := uint64(0)

	for seq := channel.Msgs.FirstSequence(); seq <= channel.Msgs.LastSequence(); seq++ {

		locatedMsg := channel.Msgs.Lookup(seq)

		var msg *pb.MsgProto
		msg = &pb.MsgProto{}
		err = msg.Unmarshal(locatedMsg.Data)

		fmt.Printf("Type: %s, Sequence: %d, Time: %d, Data: %s\n\n", locatedMsg.Subject, locatedMsg.Sequence, locatedMsg.Timestamp, locatedMsg.Data)

		if seq < min {
			min = seq
		}
		if seq > max {
			max = seq
		}

		count++
	}

	fmt.Printf("COUNT: %d\nMIN: %d\nMAX: %d\n", count, min, max)
}
