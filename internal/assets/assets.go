// Package assets embeds the Gaze web UI files into the binary using Go's
// embed directive. This means the CLI ships as a single self-contained binary.
package assets

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed web/index.html
var indexHTML string

//go:embed web/style.css
var styleCSS string

//go:embed web/app.js
var appJS string

// IndexHTML returns the full content of the receiver web page.
func IndexHTML() string {
	return indexHTML
}

// StaticHandler returns an http.Handler that serves the embedded CSS and JS.
func StaticHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		switch path {
		case "style.css":
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
			w.Write([]byte(styleCSS))
		case "app.js":
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			w.Write([]byte(appJS))
		default:
			http.NotFound(w, r)
		}
	})
}
