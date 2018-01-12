package rest

import "net/http"

type AssetHandler struct {
	root  http.FileSystem
	asset string
}

func NewAssetHandler(root http.FileSystem, asset string) *AssetHandler {
	return &AssetHandler{
		root:  root,
		asset: asset,
	}
}

func (f *AssetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	content, _ := f.root.Open(f.asset)
	info, _ := content.Stat()
	http.ServeContent(w, r, info.Name(), info.ModTime(), content)
}
