package rest

import (
	"net/http"

	"github.com/dgraph-io/badger"
	"goji.io/pat"
)

type TopicsController struct {
}

func (c *TopicsController) Register(mux *CtxMux) {
	mux.Handle(pat.Get("/topics/:topic/partitions/:partition/offset/:offset"), c.FetchMessage)
}

func (c *TopicsController) FetchMessage(ctx *WebContext, w http.ResponseWriter, req *http.Request) (interface{}, int, error) {

	topic := pat.Param(req, "topic")
	partition, err := Int32Param("partition", req)

	if err != nil {
		return nil, http.StatusBadRequest, nil
	}

	offset, err := Int64Param("offset", req)

	if err != nil {
		return nil, http.StatusBadRequest, nil
	}

	msg, err := ctx.engine.Db.FindMessage(topic, partition, offset)

	if err == nil {
		return msg, http.StatusOK, nil
	} else if err == badger.ErrKeyNotFound {
		return nil, http.StatusNotFound, nil
	}

	return nil, http.StatusInternalServerError, nil
}
