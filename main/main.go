package main

import (
	"log"
	"os"

	"github.com/dvsekhvalnov/k-ray"
	"github.com/dvsekhvalnov/k-ray/enrichment"
	. "github.com/dvsekhvalnov/k-ray/log"
	"github.com/dvsekhvalnov/k-ray/rest"
	"github.com/dvsekhvalnov/sync4kafka"
)

func start() {
	cfg := engine.NewConfig()
	cfg.BrokerUrl = "localhost:9092"

	cfg.MetadataRefreshMs = 2000
	cfg.ConsumerGroup = "k-ray-agent"
	cfg.DataDir = "./data"
	cfg.Port = ":8080"
	// cfg.Include("local.auditLog")

	engine := engine.NewEngine()
	defer engine.Close()

	engine.Enrichment.Register(&enrichment.TypeEnrichment{})
	engine.Enrichment.Register(&enrichment.BaseEnrichment{})

	api := rest.NewWebContext(engine)
	defer api.Stop()
	api.Start(cfg)

	//main loop
	if messages, err := engine.Start(cfg); err == nil {
		for _ = range messages {
			// if msg != nil {
			// 	fmt.Printf("Pass through msg: %v, paritition: %v, offset: %v, msg:%v\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
			// }
		}
	} else {
		Log.Println("Unable to start engine, got error=", err)
	}
}

func main() {
	Log = log.New(os.Stdout, "[K-RAY] [INFO] ", log.LstdFlags)
	agent.Log = log.New(os.Stdout, "[sync4kafka] [INFO] ", log.LstdFlags)

	start()
}
