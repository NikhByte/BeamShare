package assets

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticHandler_ValidAssets(t *testing.T) {
	handler := StaticHandler()

	tests := []struct {
		urlPath     string
		contentType string
		contains    string
	}{
		{"/style.css", "text/css; charset=utf-8", "body"},
		{"/app.js", "application/javascript; charset=utf-8", "function"},
		{"/pako.min.js", "application/javascript; charset=utf-8", "pako"},
	}

	for _, tc := range tests {
		t.Run(tc.urlPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.urlPath, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, tc.contentType, rr.Header().Get("Content-Type"))
			assert.True(t, strings.Contains(rr.Body.String(), tc.contains))
		})
	}
}

func TestStaticHandler_PathTraversalAndInvalidPaths(t *testing.T) {
	handler := StaticHandler()

	traversalPaths := []string{
		"/../assets.go",
		"/../../go.mod",
		"/../../../etc/passwd",
		"/..%2fassets.go",
		"/style.css/../../assets.go",
		"/app.js%00.css",
		"/unknown.js",
		"/",
		"/web/index.html",
	}

	for _, path := range traversalPaths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusNotFound, rr.Code, "Expected 404 for path: %s", path)
		})
	}
}

func TestRobotsTxtAndSitemapHandlers(t *testing.T) {
	reqRobots := httptest.NewRequest(http.MethodGet, "/robots.txt", nil)
	rrRobots := httptest.NewRecorder()
	RobotsTxtHandler(rrRobots, reqRobots)
	assert.Equal(t, http.StatusOK, rrRobots.Code)
	assert.Contains(t, rrRobots.Body.String(), "User-agent: *")
	assert.Contains(t, rrRobots.Body.String(), "Sitemap:")

	reqSitemap := httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
	rrSitemap := httptest.NewRecorder()
	SitemapXMLHandler(rrSitemap, reqSitemap)
	assert.Equal(t, http.StatusOK, rrSitemap.Code)
	assert.Contains(t, rrSitemap.Body.String(), "<urlset")
}
