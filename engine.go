package engine

import (
	"github.com/Shopify/Sarama"
	"github.com/dgraph-io/badger"
	"github.com/dvsekhvalnov/k-ray/db"
	. "github.com/dvsekhvalnov/k-ray/log"
	"github.com/dvsekhvalnov/k-ray/pool"
	"github.com/dvsekhvalnov/sync4kafka"
)

type Engine struct {
	cfg         *Config
	Db          db.Datastore
	Enrichment  EnrichmentPipeline
	lookupTable LookupTable
}

func (engine *Engine) Close() {
	engine.Db.Close()
}

func NewEngine() *Engine {
	return &Engine{
		Enrichment: EnrichmentPipeline{},
	}
}

func (engine *Engine) Start(cfg *Config) (<-chan *sarama.ConsumerMessage, error) {

	db, err := db.Open(cfg.DataDir)

	if err != nil {
		return nil, err
	}

	cfg.FetchOffsets = func(partition *agent.PartitionInfo) int64 {

		offset, err := engine.Db.FindOffset(partition.Topic, partition.ID)

		if err != nil && err != badger.ErrKeyNotFound {
			Log.Printf("[DEBUG] There was an error while fetching offset for topic=%v, partition=%v, error=%v\n", partition.Topic, partition.ID, err)
			return -1 //Fetch from beginning
		}

		Log.Printf("[DEBUG] Fetched offset=%v for topic=%v, partition=%v", offset, partition.Topic, partition.ID)

		return offset
	}

	engine.cfg = cfg
	engine.Db = db

	return Persist(agent.Sync(engine.cfg.Config), engine), nil
}

func Persist(in <-chan *sarama.ConsumerMessage, engine *Engine) <-chan *sarama.ConsumerMessage {
	out := make(chan *sarama.ConsumerMessage)

	encrichmentSpool := make(chan *db.Message, engine.cfg.Spool.Size)
	encrichmentDispatcher := pool.NewDispatcher(encrichmentSpool, engine.cfg.Spool.MaxWorkers)

	persistSpool := make(chan *db.Message, engine.cfg.Spool.Size)
	persistDispatcher := pool.NewDispatcher(persistSpool, engine.cfg.Spool.MaxWorkers)

	encrichmentDispatcher.Start(func(msg *db.Message) {
		persistSpool <- engine.Enrichment.Process(msg)
	})

	persistDispatcher.Start(func(msg *db.Message) {
		err := engine.Db.SaveMessage(msg)

		if err != nil {
			Log.Printf("[ERROR] Unable to save incoming message. Skipping. Error=", err)
		}
	})

	go func() {
		defer close(out)
		defer persistDispatcher.Stop()
		defer close(persistSpool)

		for msg := range in {
			out <- msg

			if msg != nil {
				encrichmentSpool <- db.NewMessage(msg)
			}
		}

	}()
	return out
}
