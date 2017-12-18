package engine

import (
	"github.com/dvsekhvalnov/k-ray/db"
)

type Enrichment interface {
	Process(msg *db.Message) *db.Message
}

type EnrichmentPipeline struct {
	processors []Enrichment
}

func (p *EnrichmentPipeline) Register(e Enrichment) {
	if p.processors == nil {
		p.processors = make([]Enrichment, 0)
	}

	p.processors = append(p.processors, e)
}

func (p *EnrichmentPipeline) Process(msg *db.Message) *db.Message {
	result := msg

	for _, e := range p.processors {
		result = e.Process(result)
	}

	return result
}
