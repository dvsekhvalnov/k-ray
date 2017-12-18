package rest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dvsekhvalnov/k-ray/db"
	. "github.com/dvsekhvalnov/k-ray/log"
	"goji.io/pat"
)

type EventsSearchController struct {
}

func (c *EventsSearchController) Register(mux *CtxMux) {
	mux.Handle(pat.Post("/events"), c.SearchMessages)
}

func (c *EventsSearchController) SearchMessages(ctx *WebContext, w http.ResponseWriter, req *http.Request) (interface{}, int, error) {

	search := db.SearchRequest{}

	if err := json.NewDecoder(req.Body).Decode(&search); err != nil {
		Log.Println("[ERR] Unable to decode search request:", err)
		return BadRequest()
	}

	//Validate input
	//Default interval to [-5 min;now]
	if search.Earliest == 0 {
		search.Earliest = time.Now().Add(time.Minute * time.Duration(-5)).UnixNano()
	} else {
		search.Earliest = search.Earliest * 1000000 //convert to nano
	}

	if search.Latest == 0 {
		search.Latest = time.Now().UnixNano()
	} else {
		search.Latest = search.Latest * 1000000 //convert to nano
	}

	if search.Page < 1 {
		search.Page = 1
	}

	if search.Paging == nil {
		search.Paging = &db.Paging{
			Limit: 50,
			Pages: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
	}

	if search.Paging.Limit == 0 {
		search.Paging.Limit = 50
	}

	response, err := ctx.engine.Db.SearchMessagesByTime(&search)

	if err == nil {
		return response, http.StatusOK, nil
	}

	return nil, http.StatusInternalServerError, nil
}
