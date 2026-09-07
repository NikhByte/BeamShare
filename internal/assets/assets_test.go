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
			assert.Equal(t, "public, max-age=86400, must-revalidate", rr.Header().Get("Cache-Control"))
			etag := rr.Header().Get("ETag")
			assert.NotEmpty(t, etag)
			assert.True(t, strings.HasPrefix(etag, `"`))
			assert.True(t, strings.HasSuffix(etag, `"`))
			assert.True(t, strings.Contains(rr.Body.String(), tc.contains))

			// If-None-Match with matching ETag returns 304 Not Modified
			reqCached := httptest.NewRequest(http.MethodGet, tc.urlPath, nil)
			reqCached.Header.Set("If-None-Match", etag)
			rrCached := httptest.NewRecorder()
			handler.ServeHTTP(rrCached, reqCached)
			assert.Equal(t, http.StatusNotModified, rrCached.Code)
			assert.Empty(t, rrCached.Body.String())

			// If-None-Match with mismatched ETag returns 200 OK
			reqMismatch := httptest.NewRequest(http.MethodGet, tc.urlPath, nil)
			reqMismatch.Header.Set("If-None-Match", `"wrong-etag"`)
			rrMismatch := httptest.NewRecorder()
			handler.ServeHTTP(rrMismatch, reqMismatch)
			assert.Equal(t, http.StatusOK, rrMismatch.Code)
			assert.NotEmpty(t, rrMismatch.Body.String())
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
