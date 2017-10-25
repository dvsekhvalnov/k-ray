package pool

import "github.com/dvsekhvalnov/k-ray/db"

type Dispatcher struct {
	queue      chan *db.Message
	pool       chan chan *db.Message
	maxWorkers int
	workers    []*Worker
}

type Worker struct {
	pool   chan chan *db.Message
	queue  chan *db.Message
	quit   chan bool
	lambda func(msg *db.Message)
}

func NewWorker(pool chan chan *db.Message, lambda func(msg *db.Message)) *Worker {
	return &Worker{
		lambda: lambda,
		pool:   pool,
		queue:  make(chan *db.Message),
		quit:   make(chan bool),
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			//register ourselves as a free worker
			w.pool <- w.queue

			select {
			case job := <-w.queue:
				w.lambda(job)
			case <-w.quit:
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	go func() {
		w.quit <- true //finish processing in-flight, then quit
	}()
}

func NewDispatcher(queue chan *db.Message, maxWorkers int) *Dispatcher {
	return &Dispatcher{
		maxWorkers: maxWorkers,
		queue:      queue,
		pool:       make(chan chan *db.Message, maxWorkers),
		workers:    make([]*Worker, 0),
	}
}

func (d *Dispatcher) Start(lambda func(msg *db.Message)) {
	for i := 0; i < d.maxWorkers; i++ {
		w := NewWorker(d.pool, lambda)
		d.workers = append(d.workers, w)
		w.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case msg := <-d.queue:
			go func(msg *db.Message) {
				//get free worker, block if no one avaliable
				worker := <-d.pool

				//dispatch msg
				worker <- msg
			}(msg)
		}
	}
}

func (d *Dispatcher) Stop() {
	for _, w := range d.workers {
		w.Stop()
	}
}
