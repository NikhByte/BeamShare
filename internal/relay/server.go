package relay

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beamshare/beam/internal/assets"
)

type Session struct {
	ID         string
	Offer      string
	Candidates []map[string]interface{}
	Meta       map[string]interface{}

	AnswerReady chan string
	DownloadReq chan struct{}

	DataPipeR *io.PipeReader
	DataPipeW *io.PipeWriter

	mu sync.Mutex
}

type Server struct {
	sessions map[string]*Session
	mu       sync.Mutex
}

func NewServer() *Server {
	return &Server{
		sessions: make(map[string]*Session),
	}
}

func (s *Server) getSession(id string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *Server) createSession() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := fmt.Sprintf("%06d", rand.Intn(1000000))
	sess := &Session{
		ID:          id,
		AnswerReady: make(chan string, 1),
		DownloadReq: make(chan struct{}, 1),
	}
	s.sessions[id] = sess

	// Optional: cleanup after timeout
	go func() {
		time.Sleep(2 * time.Hour)
		s.mu.Lock()
		delete(s.sessions, id)
		s.mu.Unlock()
	}()

	return sess
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Range")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	mux := http.NewServeMux()

	// --- Relay Control Endpoints (Sender -> Relay) ---
	mux.HandleFunc("/relay/register", s.handleRegister)
	mux.HandleFunc("/relay/state", s.handleState)
	mux.HandleFunc("/relay/poll", s.handlePoll)
	mux.HandleFunc("/relay/data", s.handleData)

	// --- Public UI Endpoints (Receiver -> Relay) ---
	mux.HandleFunc("/", s.handleUI)
	mux.HandleFunc("/api/meta", s.handleMeta)
	mux.HandleFunc("/api/signal/offer", s.handleOffer)
	mux.HandleFunc("/api/signal/answer", s.handleAnswer)
	mux.HandleFunc("/api/signal/candidates", s.handleCandidates)
	mux.HandleFunc("/api/download", s.handleDownload)

	// Add support for QR API since app.js requests it (we can just return an empty image or real one)
	mux.HandleFunc("/api/qr", s.handleQR)

	mux.ServeHTTP(w, r)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	sess := s.createSession()
	json.NewEncoder(w).Encode(map[string]string{"session": sess.ID})
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("session"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	var req struct {
		Offer      string                   `json:"offer"`
		Candidates []map[string]interface{} `json:"candidates"`
		Meta       map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	sess.mu.Lock()
	if req.Offer != "" {
		sess.Offer = req.Offer
	}
	if req.Candidates != nil {
		sess.Candidates = req.Candidates
	}
	if req.Meta != nil {
		sess.Meta = req.Meta
	}
	sess.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("session"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	select {
	case answer := <-sess.AnswerReady:
		json.NewEncoder(w).Encode(map[string]interface{}{
			"action": "answer",
			"answer": answer,
		})
	case <-sess.DownloadReq:
		json.NewEncoder(w).Encode(map[string]interface{}{
			"action": "download",
		})
	case <-r.Context().Done():
		return
	}
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("session"))
	if sess == nil || sess.DataPipeW == nil {
		http.Error(w, "not found or pipe not ready", 404)
		return
	}

	_, err := io.Copy(sess.DataPipeW, r.Body)
	sess.DataPipeW.CloseWithError(err)
}

// --- Receiver Handlers ---

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	// Serve assets
	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(assets.IndexHTML()))
		return
	}
	if r.URL.Path == "/sw.js" {
		w.Header().Set("Content-Type", "application/javascript")
		w.Write([]byte(assets.ServiceWorkerJS()))
		return
	}
	if r.URL.Path == "/robots.txt" {
		assets.RobotsTxtHandler(w, r)
		return
	}
	if r.URL.Path == "/sitemap.xml" {
		assets.SitemapXMLHandler(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/static/") {
		http.StripPrefix("/static/", assets.StaticHandler()).ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(sess.Meta)
}

func (s *Server) handleOffer(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]string{
		"sdp":  sess.Offer,
		"type": "offer",
	})
}

func (s *Server) handleAnswer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		return
	}

	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	body, _ := io.ReadAll(r.Body)
	select {
	case sess.AnswerReady <- string(body):
	default:
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCandidates(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(sess.Candidates)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	sess.mu.Lock()
	pr, pw := io.Pipe()
	sess.DataPipeR = pr
	sess.DataPipeW = pw
	sess.mu.Unlock()

	// Notify sender
	select {
	case sess.DownloadReq <- struct{}{}:
	default:
	}

	// Stream data from sender to receiver
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	sess.mu.Lock()
	metaSize := sess.Meta["size"]
	sess.mu.Unlock()
	if metaSize != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%v", metaSize))
	}

	io.Copy(w, pr)
}

func (s *Server) handleQR(w http.ResponseWriter, r *http.Request) {
	// A dummy QR API to prevent 404s
	w.WriteHeader(200)
}
