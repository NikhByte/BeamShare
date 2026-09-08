package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beamshare/beam/internal/assets"
)

type DownloadRequest struct {
	Offset int64  `json:"offset,omitempty"`
	Range  string `json:"range,omitempty"`
}

type Session struct {
	ID         string
	Offer      string
	Candidates []map[string]interface{}
	Meta       map[string]interface{}

	AnswerReady chan string
	DownloadReq chan DownloadRequest

	DataPipeR *io.PipeReader
	DataPipeW *io.PipeWriter

	RequestedOffset int64
	SenderOffset    int64

	SenderOffsetReady chan struct{}

	expiresAt time.Time
	mu        sync.Mutex
}

func (s *Session) ClosePipes(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.SenderOffsetReady != nil {
		select {
		case <-s.SenderOffsetReady:
		default:
			close(s.SenderOffsetReady)
		}
		s.SenderOffsetReady = nil
	}
	if s.DataPipeW != nil {
		if err != nil {
			s.DataPipeW.CloseWithError(err)
		} else {
			s.DataPipeW.Close()
		}
		s.DataPipeW = nil
	}
	if s.DataPipeR != nil {
		if err != nil {
			s.DataPipeR.CloseWithError(err)
		} else {
			s.DataPipeR.Close()
		}
		s.DataPipeR = nil
	}
}

func (s *Session) IsPipeReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DataPipeR != nil && s.DataPipeW != nil
}

type Server struct {
	sessions           map[string]*Session
	sessionTTL         time.Duration
	cleanupInterval    time.Duration
	stopChan           chan struct{}
	ticker             *time.Ticker
	wg                 sync.WaitGroup
	SessionIDGenerator func() string
	mu                 sync.Mutex
}

func NewServer() *Server {
	return NewServerWithConfig(2*time.Hour, 1*time.Minute)
}

func NewServerWithConfig(ttl, cleanupInterval time.Duration) *Server {
	s := &Server{
		sessions:        make(map[string]*Session),
		sessionTTL:      ttl,
		cleanupInterval: cleanupInterval,
		stopChan:        make(chan struct{}),
	}
	s.startSweeper()
	return s
}

func (s *Server) startSweeper() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startSweeperLocked()
}

func (s *Server) startSweeperLocked() {
	if s.cleanupInterval <= 0 || s.ticker != nil {
		return
	}
	if s.stopChan == nil {
		s.stopChan = make(chan struct{})
	}
	ticker := time.NewTicker(s.cleanupInterval)
	s.ticker = ticker

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ticker.C:
				s.SweepExpiredSessions()
			case <-s.stopChan:
				return
			}
		}
	}()
}

func (s *Server) Stop() {
	s.mu.Lock()
	if s.ticker != nil {
		s.ticker.Stop()
		close(s.stopChan)
		s.ticker = nil
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) SweepExpiredSessions() {
	s.mu.Lock()
	now := time.Now()
	var expired []*Session
	for id, sess := range s.sessions {
		sess.mu.Lock()
		exp := sess.expiresAt
		sess.mu.Unlock()
		if !exp.IsZero() && now.After(exp) {
			expired = append(expired, sess)
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()

	for _, sess := range expired {
		sess.ClosePipes(fmt.Errorf("session expired"))
	}
}

func (s *Server) GetSession(id string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		return nil
	}
	return s.sessions[id]
}

func (s *Server) getSession(id string) *Session {
	return s.GetSession(id)
}

func (s *Server) createSession() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sessions == nil {
		s.sessions = make(map[string]*Session)
	}
	if s.sessionTTL <= 0 {
		s.sessionTTL = 2 * time.Hour
	}
	if s.cleanupInterval <= 0 {
		s.cleanupInterval = 1 * time.Minute
	}
	s.startSweeperLocked()

	var id string
	for {
		if s.SessionIDGenerator != nil {
			id = s.SessionIDGenerator()
		} else {
			b := make([]byte, 16)
			if _, err := rand.Read(b); err != nil {
				id = fmt.Sprintf("%x", time.Now().UnixNano())
			} else {
				id = hex.EncodeToString(b)
			}
		}
		if _, exists := s.sessions[id]; !exists {
			break
		}
	}

	sess := &Session{
		ID:                id,
		AnswerReady:       make(chan string, 1),
		DownloadReq:       make(chan DownloadRequest, 1),
		SenderOffsetReady: make(chan struct{}),
		expiresAt:         time.Now().Add(s.sessionTTL),
	}
	s.sessions[id] = sess

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
	if r.Method == http.MethodOptions {
		return
	}
	sess := s.createSession()
	json.NewEncoder(w).Encode(map[string]string{
		"session":    sess.ID,
		"session_id": sess.ID,
	})
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
		sanitizedCandidates := make([]map[string]interface{}, 0, len(req.Candidates))
		for _, c := range req.Candidates {
			if c != nil {
				sanitizedCandidates = append(sanitizedCandidates, c)
			}
		}
		sess.Candidates = sanitizedCandidates
	} else if sess.Candidates == nil {
		sess.Candidates = []map[string]interface{}{}
	}
	if req.Meta != nil {
		sanitizedMeta := make(map[string]interface{})
		for k, v := range req.Meta {
			if k == "" {
				continue
			}
			switch val := v.(type) {
			case string:
				sanitizedMeta[k] = val
			case float64:
				sanitizedMeta[k] = int64(val)
			case int64, int, bool:
				sanitizedMeta[k] = val
			}
		}
		sess.Meta = sanitizedMeta
	} else if sess.Meta == nil {
		sess.Meta = map[string]interface{}{}
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
	case dlReq := <-sess.DownloadReq:
		resp := map[string]interface{}{
			"action": "download",
		}
		if dlReq.Offset > 0 {
			resp["offset"] = dlReq.Offset
		}
		if dlReq.Range != "" {
			resp["range"] = dlReq.Range
		}
		json.NewEncoder(w).Encode(resp)
	case <-r.Context().Done():
		return
	}
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("session"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	sess.mu.Lock()
	pw := sess.DataPipeW
	pr := sess.DataPipeR
	senderOffset := parseSenderOffset(r, 0)
	sess.SenderOffset = senderOffset
	if sess.SenderOffsetReady != nil {
		select {
		case <-sess.SenderOffsetReady:
		default:
			close(sess.SenderOffsetReady)
		}
	}
	sess.mu.Unlock()

	if pw == nil || pr == nil {
		http.Error(w, "not found or pipe not ready", 404)
		return
	}

	var copyErr error
	defer func() {
		if copyErr != nil {
			sess.ClosePipes(copyErr)
		}
	}()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-r.Context().Done():
			sess.ClosePipes(fmt.Errorf("sender context cancelled: %w", r.Context().Err()))
		case <-done:
		}
	}()

	_, copyErr = io.Copy(pw, r.Body)
	if copyErr == nil {
		pw.Close()
	}
}

type byteRange struct {
	start int64
	end   int64 // inclusive; -1 if open-ended or totalSize unknown
}

func parseRangeHeader(rangeHdr string, totalSize int64) (hasRange bool, r byteRange, unsatisfiable bool) {
	if rangeHdr == "" || !strings.HasPrefix(rangeHdr, "bytes=") {
		return false, byteRange{}, false
	}

	spec := strings.TrimPrefix(rangeHdr, "bytes=")
	if idx := strings.Index(spec, ","); idx != -1 {
		spec = spec[:idx]
	}
	spec = strings.TrimSpace(spec)

	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return true, byteRange{}, true
	}

	var start, end int64 = -1, -1

	if parts[0] != "" {
		parsedStart, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || parsedStart < 0 {
			return true, byteRange{}, true
		}
		start = parsedStart
	}

	if parts[1] != "" {
		parsedEnd, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || parsedEnd < 0 {
			return true, byteRange{}, true
		}
		end = parsedEnd
	}

	if start == -1 && end == -1 {
		return true, byteRange{}, true
	}

	// Suffix range: bytes=-500
	if start == -1 {
		if totalSize <= 0 {
			return true, byteRange{}, true
		}
		suffixLen := end
		if suffixLen <= 0 {
			return true, byteRange{}, true
		}
		start = totalSize - suffixLen
		if start < 0 {
			start = 0
		}
		end = totalSize - 1
	} else if end == -1 {
		// Open-ended range: bytes=500-
		if totalSize > 0 {
			end = totalSize - 1
		}
	}

	if start > end && end != -1 {
		return true, byteRange{}, true
	}

	if totalSize > 0 {
		if start >= totalSize {
			return true, byteRange{}, true
		}
		if end >= totalSize {
			end = totalSize - 1
		}
	}

	return true, byteRange{start: start, end: end}, false
}

func parseSenderOffset(r *http.Request, defaultOffset int64) int64 {
	if offStr := r.URL.Query().Get("offset"); offStr != "" {
		if val, err := strconv.ParseInt(offStr, 10, 64); err == nil && val >= 0 {
			return val
		}
	}
	if cr := r.Header.Get("Content-Range"); cr != "" && strings.HasPrefix(cr, "bytes ") {
		spec := strings.TrimPrefix(cr, "bytes ")
		parts := strings.Split(spec, "-")
		if len(parts) > 0 {
			if val, err := strconv.ParseInt(parts[0], 10, 64); err == nil && val >= 0 {
				return val
			}
		}
	}
	if rng := r.Header.Get("Range"); rng != "" && strings.HasPrefix(rng, "bytes=") {
		spec := strings.TrimPrefix(rng, "bytes=")
		parts := strings.Split(spec, "-")
		if len(parts) > 0 && parts[0] != "" {
			if val, err := strconv.ParseInt(parts[0], 10, 64); err == nil && val >= 0 {
				return val
			}
		}
	}
	if offHdr := r.Header.Get("X-Offset"); offHdr != "" {
		if val, err := strconv.ParseInt(offHdr, 10, 64); err == nil && val >= 0 {
			return val
		}
	}
	return defaultOffset
}

// --- Receiver Handlers ---

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	// Serve assets
	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write([]byte(assets.IndexHTML()))
		return
	}
	if r.URL.Path == "/sw.js" {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Header().Set("Service-Worker-Allowed", "/")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
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
	meta := sess.Meta
	if meta == nil {
		meta = map[string]interface{}{
			"name": "unknown",
			"size": int64(0),
			"mime": "application/octet-stream",
		}
	}
	sess.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(meta)
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
	candidates := sess.Candidates
	if candidates == nil {
		candidates = []map[string]interface{}{}
	}
	sess.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(candidates)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r.URL.Query().Get("s"))
	if sess == nil {
		http.Error(w, "not found", 404)
		return
	}

	rangeHdr := r.Header.Get("Range")

	sess.mu.Lock()
	var totalSize int64
	if sess.Meta != nil {
		if sVal, ok := sess.Meta["size"].(int64); ok {
			totalSize = sVal
		} else if fVal, ok := sess.Meta["size"].(float64); ok {
			totalSize = int64(fVal)
		}
	}
	sess.mu.Unlock()

	hasRange, rng, unsatisfiable := parseRangeHeader(rangeHdr, totalSize)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges, Content-Disposition")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")

	if unsatisfiable {
		if totalSize > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
		}
		http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	var offset int64
	if hasRange {
		offset = rng.start
		if totalSize > 0 && rng.end >= rng.start {
			contentLen := rng.end - rng.start + 1
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, totalSize))
			if contentLen < totalSize {
				w.Header().Set("Content-Length", strconv.FormatInt(contentLen, 10))
			}
		} else {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-/*", rng.start))
		}
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	pr, pw := io.Pipe()
	offsetReady := make(chan struct{})

	sess.mu.Lock()
	if sess.DataPipeR != nil || sess.DataPipeW != nil {
		sess.ClosePipes(fmt.Errorf("replaced by new download request"))
	}
	sess.DataPipeR = pr
	sess.DataPipeW = pw
	sess.RequestedOffset = offset
	sess.SenderOffset = 0
	sess.SenderOffsetReady = offsetReady
	sess.mu.Unlock()

	var downloadErr error
	defer func() {
		sess.ClosePipes(downloadErr)
	}()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-r.Context().Done():
			sess.ClosePipes(fmt.Errorf("receiver context cancelled: %w", r.Context().Err()))
		case <-done:
		}
	}()

	// Notify sender with offset/range
	select {
	case sess.DownloadReq <- DownloadRequest{Offset: offset, Range: rangeHdr}:
	default:
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	var streamReader io.Reader = &seekingReader{pr: pr, sess: sess, offsetReady: offsetReady, ctx: r.Context()}
	if hasRange && rng.end >= rng.start {
		limit := rng.end - rng.start + 1
		streamReader = io.LimitReader(streamReader, limit)
	}

	buf := make([]byte, 32*1024)
	for {
		n, errRead := streamReader.Read(buf)
		if n > 0 {
			_, errWrite := w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if errWrite != nil {
				downloadErr = errWrite
				sess.ClosePipes(errWrite)
				break
			}
		}
		if errRead != nil {
			if errRead != io.EOF {
				downloadErr = errRead
				sess.ClosePipes(errRead)
			}
			break
		}
	}
}

type seekingReader struct {
	pr          *io.PipeReader
	sess        *Session
	offsetReady chan struct{}
	ctx         context.Context
	skipped     bool
}

func (sr *seekingReader) Read(p []byte) (int, error) {
	if sr.ctx != nil && sr.ctx.Err() != nil {
		return 0, sr.ctx.Err()
	}
	if !sr.skipped {
		sr.skipped = true
		if sr.offsetReady != nil {
			select {
			case <-sr.offsetReady:
			case <-sr.ctx.Done():
				return 0, sr.ctx.Err()
			}
		}
		if sr.ctx != nil && sr.ctx.Err() != nil {
			return 0, sr.ctx.Err()
		}
		sr.sess.mu.Lock()
		reqOff := sr.sess.RequestedOffset
		sendOff := sr.sess.SenderOffset
		sr.sess.mu.Unlock()

		bytesToSkip := reqOff - sendOff
		if bytesToSkip > 0 {
			if _, err := io.CopyN(io.Discard, sr.pr, bytesToSkip); err != nil {
				return 0, err
			}
		}
	}
	if sr.ctx != nil && sr.ctx.Err() != nil {
		return 0, sr.ctx.Err()
	}
	return sr.pr.Read(p)
}

func (s *Server) handleQR(w http.ResponseWriter, r *http.Request) {
	// A dummy QR API to prevent 404s
	w.WriteHeader(200)
}
