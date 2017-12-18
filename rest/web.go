package rest

import (
	"net/http"
	"strconv"

	goji "goji.io"
	"goji.io/pat"
)

type Action struct {
	*WebContext
	Apply func(*WebContext, http.ResponseWriter, *http.Request) (interface{}, int, error)
}

type CtxMux struct {
	*goji.Mux
	*WebContext
}

func NewMux(c *WebContext) *CtxMux {
	return &CtxMux{goji.NewMux(), c}
}

func (m *CtxMux) Handle(p goji.Pattern, h func(*WebContext, http.ResponseWriter, *http.Request) (interface{}, int, error)) {
	m.Mux.Handle(p, Action{m.WebContext, h})
}

func (action Action) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, status, err := action.Apply(action.WebContext, w, r)

	if err != nil {
		switch status {
		case http.StatusNotFound:
			http.NotFound(w, r)
		default:
			http.Error(w, http.StatusText(status), status)
		}
	} else if body != nil {
		action.WebContext.JSON(w, status, body)
	} else {
		w.WriteHeader(status)
	}
}

func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func Int32Param(name string, req *http.Request) (int32, error) {
	val, err := strconv.ParseInt(pat.Param(req, name), 10, 32)

	if err != nil {
		return 0, err
	}

	return int32(val), nil
}

func Int64Param(name string, req *http.Request) (int64, error) {
	val, err := strconv.ParseInt(pat.Param(req, name), 10, 64)

	if err != nil {
		return 0, err
	}

	return val, nil
}

func Int64QueryParam(name string, req *http.Request) (int64, error) {
	if param, ok := req.URL.Query()[name]; ok {
		val, err := strconv.ParseInt(param[0], 10, 64)

		if err != nil {
			return 0, err
		}

		return val, nil
	}

	return 0, nil
}

func BadRequest() (interface{}, int, error) {
	return nil, http.StatusBadRequest, nil
}
