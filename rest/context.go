package rest

import (
	"net/http"

	"github.com/dvsekhvalnov/k-ray"
	"github.com/dvsekhvalnov/k-ray/assets"
	. "github.com/dvsekhvalnov/k-ray/log"
	"github.com/rs/cors"
	"github.com/unrolled/render"
	"goji.io/pat"
)

type WebContext struct {
	engine *engine.Engine
	server *http.Server
	*render.Render
}

func NewWebContext(engine *engine.Engine) *WebContext {

	ctx := &WebContext{
		engine: engine,
		Render: render.New(render.Options{}),
		server: &http.Server{},
	}

	topics := &TopicsController{}
	events := &EventsSearchController{}

	mux := NewMux(ctx)

	//CORS
	ctx.server.Handler = cors.Default().Handler(mux)

	//API
	topics.Register(mux)
	events.Register(mux)

	//static UI app (js, css, e.t.c.)
	assets := assets.FS(false)
	mux.Mux.Handle(pat.Get("/:file.:ext"), http.FileServer(assets))
	mux.Mux.Handle(pat.Get("/*"), NewAssetHandler(assets, "/index.html"))

	return ctx
}

func (ctx *WebContext) Start(cfg *engine.Config) {

	ctx.server.Addr = cfg.Port

	Log.Println("Starting HTTP server.")
	go func() {
		err := ctx.server.ListenAndServe()

		if err != http.ErrServerClosed {
			Log.Println("HTTP server error:", err)
		} else {
			Log.Println("HTTP server stopped. ")
		}
	}()
}

func (ctx *WebContext) Stop() {
	ctx.server.Close()
}
