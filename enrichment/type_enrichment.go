package enrichment

import (
	"encoding/json"

	"github.com/dvsekhvalnov/k-ray/db"
)

type TypeEnrichment struct {
}

func (e *TypeEnrichment) Process(msg *db.Message) *db.Message {

	msg.Value.Type = e.probe(&msg.Value)
	msg.Key.Type = e.probe(&msg.Key)

	return msg
}

func (e *TypeEnrichment) probe(blob *db.BinaryData) string {
	var data map[string]interface{}

	err := json.Unmarshal(blob.Value, &data)

	if err != nil {
		return "binary"
	}

	return "json"
}
