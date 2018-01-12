package main

import (
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/dvsekhvalnov/k-ray"
	"github.com/dvsekhvalnov/k-ray/enrichment"
	. "github.com/dvsekhvalnov/k-ray/log"
	"github.com/dvsekhvalnov/k-ray/rest"
	"github.com/dvsekhvalnov/sync4kafka"
	"github.com/spf13/viper"
)

func dumpConfig(cfg *engine.Config) {
	Log.Println("Effective configuration:")
	Log.Printf("\tBrokerUrl = %v\n", cfg.Agent.BrokerUrl)
	Log.Printf("\tBrokerAdvertisedUrl = %v\n", cfg.Agent.BrokerAdvertisedUrl)
	Log.Printf("\tMetadataRefreshMs = %v\n", cfg.Agent.MetadataRefreshMs)
	Log.Printf("\tConsumerGroup = %v\n", cfg.Agent.ConsumerGroup)
	Log.Printf("\tDataDir = %v\n", cfg.DataDir)
	Log.Printf("\tPort = %v\n", cfg.Port)
}

func start() {

	viper.SetConfigName("k-ray")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/k-ray")
	viper.SetDefault("agent.brokerUrl", "localhost:9092")
	viper.SetDefault("agent.MetadataRefreshMs", 2000)
	viper.SetDefault("agent.ConsumerGroup", "k-ray-agent")
	viper.SetDefault("DataDir", "./data")
	viper.SetDefault("Port", ":8080")

	if err := viper.ReadInConfig(); err != nil {
		Log.Println("No configuration file found, using defaults")
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("kray")
	viper.AutomaticEnv()

	cfg := engine.NewConfig()

	if err := viper.Unmarshal(&cfg); err != nil {
		Log.Println("There were problems reading configuration, err=", err)
	}

	dumpConfig(cfg)

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
			// Log.Println(m.Topic, ":", m.Partition, ":", m.Offset, ":", m.Timestamp)
		}
	} else {
		Log.Println("Unable to start engine, got error=", err)
	}
}

func main() {
	Log = log.New(os.Stdout, "[K-RAY] [INFO] ", log.LstdFlags)
	agent.Log = log.New(os.Stdout, "[sync4kafka] [INFO] ", log.LstdFlags)

	runtime.GOMAXPROCS(128)

	start()
}
