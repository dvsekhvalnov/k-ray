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
	cfg *Config
	Db  db.Datastore
}

func (engine *Engine) Close() {
	engine.Db.Close()
}

func NewEngine() *Engine {
	return &Engine{}
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
	spool := make(chan *db.Message, engine.cfg.Spool.Size)
	dispatcher := pool.NewDispatcher(spool, engine.cfg.Spool.MaxWorkers)

	dispatcher.Start(func(msg *db.Message) {
		err := engine.Db.Save(msg)

		if err != nil {
			Log.Printf("[ERROR] Unable to save incoming message. Skipping. Error=", err)
		}
	})

	go func() {
		defer close(out)
		defer dispatcher.Stop()
		defer close(spool)

		for msg := range in {
			out <- msg

			if msg != nil {
				spool <- db.NewMessage(msg)
			}
		}

	}()
	return out
}
