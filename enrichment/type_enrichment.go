package enrichment

import (
	"encoding/json"

	"github.com/dvsekhvalnov/k-ray/db"
)

type TypeEnrichment struct {
}

func (e *TypeEnrichment) Process(msg *db.Message) *db.Message {
	var data map[string]interface{}

	err := json.Unmarshal(msg.Value.Value, &data)

	if err != nil {
		msg.Type = "binary"
	} else {
		msg.Type = "json"
	}

	return msg
}
