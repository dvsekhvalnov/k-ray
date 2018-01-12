package rest

import (
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/dgraph-io/badger"
	"goji.io/pat"
)

type TopicsController struct {
}

func (c *TopicsController) Register(mux *CtxMux) {
	mux.Handle(pat.Get("/topics/:topic/partitions/:partition/offset/:offset"), c.FetchMessage)
	mux.Handle(pat.Get("/topics/:topic/partitions/:partition/offset/:offset/download"), c.DownloadMessage)
}

func (c *TopicsController) DownloadMessage(ctx *WebContext, w http.ResponseWriter, req *http.Request) (interface{}, int, error) {
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
		ext := ".json"
		content := msg.Value.Value

		if msg.Type == "binary" {
			ext = ".bin"
			content = []byte(base64.StdEncoding.EncodeToString(msg.Value.Value))
		}

		filename := msg.Topic + "-" + strconv.FormatInt(int64(msg.Partition), 10) + "-" + strconv.FormatInt(int64(msg.Offset), 10) + ext
		w.Header().Set("Content-Disposition", "attachment; filename="+filename)
		// w.Header().Set("Content-Type", req.Header.Get("Content-Type"))

		// fmt.Fprint(w, content)

		return content, http.StatusOK, nil
	} else if err == badger.ErrKeyNotFound {
		return nil, http.StatusNotFound, nil
	}

	return nil, http.StatusInternalServerError, nil
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
