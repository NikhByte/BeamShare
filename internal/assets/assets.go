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

//go:embed web/sw.js
var swJS string

//go:embed web/pako.min.js
var pakoJS string

// IndexHTML returns the full content of the receiver web page.
func IndexHTML() string {
	return indexHTML
}

// ServiceWorkerJS returns the content of the service worker file.
func ServiceWorkerJS() string {
	return swJS
}

// StyleCSS returns the style.css content.
func StyleCSS() string {
	return styleCSS
}

// AppJS returns the app.js content.
func AppJS() string {
	return appJS
}

// PakoJS returns the pako.min.js content.
func PakoJS() string {
	return pakoJS
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
		case "pako.min.js":
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			w.Write([]byte(pakoJS))
		default:
			http.NotFound(w, r)
		}
	})
}

// RobotsTxtHandler serves a dynamic robots.txt file.
func RobotsTxtHandler(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = "localhost"
	}

	content := "User-agent: *\n" +
		"Allow: /\n" +
		"Sitemap: " + scheme + "://" + host + "/sitemap.xml\n"

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(content))
}

// SitemapXMLHandler serves a dynamic sitemap.xml file.
func SitemapXMLHandler(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = "localhost"
	}

	content := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>` + scheme + `://` + host + `/</loc>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>`

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Write([]byte(content))
}
