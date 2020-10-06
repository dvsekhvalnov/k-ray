package enrichment

import (
	"encoding/json"

	"github.com/dvsekhvalnov/k-ray/db"
)

type JsonMessage struct {
	Context struct {
		CorrelationId string `json:"correlationId"`
		MessageType   string `json:"messageType"`
		Operation     string `json:"operation"`

		Origin struct {
			Dc      string `json:"dc"`
			Host    string `json:"host"`
			Service string `json:"service"`
		}
	}
}

type BaseEnrichment struct {
}

func (e *BaseEnrichment) Process(msg *db.Message) *db.Message {
	if msg.Value.Type == "json" {
		var jsonMsg JsonMessage

		if err := json.Unmarshal(msg.Value.Value, &jsonMsg); err == nil {

			msg.AddTag("messageType", jsonMsg.Context.MessageType, jsonMsg.Context.Operation)
			msg.AddTag("host", jsonMsg.Context.Origin.Host, "")
			msg.AddTag("correlationId", jsonMsg.Context.CorrelationId, "")
			msg.AddTag("service", jsonMsg.Context.Origin.Service, "")
		}
	}

	return msg
}
