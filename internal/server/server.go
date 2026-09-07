// Package server implements the local HTTP server that serves the Gaze
// receiver UI, the file-download API, and (Phase 3+) the WebRTC signaling API.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/beamshare/beam/internal/assets"
	qrcode "github.com/skip2/go-qrcode"
)

// Server holds the state for one Beam session.
type Server struct {
	filePath     string
	fileName     string
	fileSize     int64
	port         int
	srv          *http.Server
	mux          *http.ServeMux
	mu           sync.Mutex
	downloads    int

	// Phase 5: Live Pipe
	isLivePipe     bool
	liveBuf        *RingBuffer
	liveClients    []chan []byte
	liveFinished   bool
}

// FileMeta is the JSON response for /api/meta.
type FileMeta struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	MIME string `json:"mime"`
}

// New creates and configures a new Server. If filePath is empty,
// it runs in Live Pipe mode.
func New(filePath string, bufferSize int) (*Server, error) {
	var fileName string
	var fileSize int64
	var isLive bool

	if filePath != "" {
		info, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("stat file: %w", err)
		}
		fileName = filepath.Base(filePath)
		fileSize = info.Size()
	} else {
		fileName = "live.log"
		fileSize = -1 // -1 signifies live stream / pipe
		isLive = true
	}

	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("find free port: %w", err)
	}

	mux := http.NewServeMux()

	s := &Server{
		filePath:       filePath,
		fileName:       fileName,
		fileSize:       fileSize,
		port:           port,
		mux:            mux,
		isLivePipe:     isLive,
	}

	if isLive {
		s.liveBuf = NewRingBuffer(bufferSize)
	}

	// Core routes.
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/sw.js", s.handleServiceWorker)
	mux.HandleFunc("/robots.txt", assets.RobotsTxtHandler)
	mux.HandleFunc("/sitemap.xml", assets.SitemapXMLHandler)
	mux.HandleFunc("/api/meta", s.handleMeta)
	mux.HandleFunc("/api/download", s.handleDownload)
	mux.HandleFunc("/api/upload", s.handleUpload)
	mux.HandleFunc("/api/qr", s.handleQR)

	if isLive {
		mux.HandleFunc("/api/live/stream", s.handleLiveStream)
	}

	// Static assets (CSS, JS) embedded at compile time.
	mux.Handle("/static/", http.StripPrefix("/static/", assets.StaticHandler()))

	s.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  0, // disable read timeout for large uploads
		WriteTimeout: 0, // disable write timeout for large downloads
		IdleTimeout:  120 * time.Second,
	}

	return s, nil
}

// WriteLive broadcasts new input to all connected SSE clients.
func (s *Server) WriteLive(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.liveBuf != nil {
		s.liveBuf.Write(data)
	}

	for _, ch := range s.liveClients {
		// Non-blocking write to client channel
		select {
		case ch <- data:
		default:
		}
	}
}

// CloseLive signals to all SSE clients that the stream is finished.
func (s *Server) CloseLive() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.liveFinished = true
	for _, ch := range s.liveClients {
		close(ch)
	}
	s.liveClients = nil
}

// GetLiveBacklog returns the accumulated stream buffer.
func (s *Server) GetLiveBacklog() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveBuf != nil {
		return s.liveBuf.Bytes()
	}
	return nil
}

// Mux returns the underlying ServeMux.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// LocalURL returns the http://lan-ip:port URL for this session.
func (s *Server) LocalURL() string {
	return fmt.Sprintf("http://%s:%d", GetLocalIP(), s.port)
}

// Port returns the bound port.
func (s *Server) Port() int { return s.port }

// Serve starts listening.
func (s *Server) Serve() error {
	return s.srv.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ─── handlers ────────────────────────────────────────────────────────────────

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write([]byte(assets.IndexHTML()))
}

func (s *Server) handleServiceWorker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Service-Worker-Allowed", "/")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Write([]byte(assets.ServiceWorkerJS()))
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(FileMeta{
		Name: s.fileName,
		Size: s.fileSize,
		MIME: guessMIME(s.fileName),
	})
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Range")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.mu.Lock()
	s.downloads++
	count := s.downloads
	s.mu.Unlock()
	fmt.Printf("\r  Receiver connected (download #%d)…\n", count)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Content-Disposition, Accept-Ranges")
	w.Header().Set("Accept-Ranges", "bytes")

	if s.isLivePipe {
		s.mu.Lock()
		var data []byte
		if s.liveBuf != nil {
			data = s.liveBuf.Bytes()
		}
		s.mu.Unlock()

		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, s.fileName))
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		http.ServeContent(w, r, s.fileName, time.Time{}, bytes.NewReader(data))
		return
	}

	f, err := os.Open(s.filePath)
	if err != nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	var modTime time.Time
	if err == nil {
		modTime = stat.ModTime()
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, s.fileName))
	w.Header().Set("Content-Type", guessMIME(s.fileName))

	http.ServeContent(w, r, s.fileName, modTime, f)
}

func (s *Server) handleLiveStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	flusher.Flush()

	ch := make(chan []byte, 100)

	var backlog []byte
	var finished bool

	s.mu.Lock()
	if s.liveBuf != nil {
		backlog = s.liveBuf.Bytes()
	}
	finished = s.liveFinished
	if !finished {
		s.liveClients = append(s.liveClients, ch)
	}
	s.mu.Unlock()

	// Send backlog outside mutex
	if len(backlog) > 0 {
		backlogEvent := map[string]interface{}{
			"type":    "backlog",
			"payload": string(backlog),
		}
		if jsonBytes, err := json.Marshal(backlogEvent); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
			flusher.Flush()
		}
	}

	if finished {
		eofEvent := map[string]interface{}{"type": "eof"}
		if jsonBytes, err := json.Marshal(eofEvent); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
			flusher.Flush()
		}
		return
	}

	defer func() {
		s.mu.Lock()
		// Remove client channel
		for i, c := range s.liveClients {
			if c == ch {
				s.liveClients = append(s.liveClients[:i], s.liveClients[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
	}()

	for {
		select {
		case data, ok := <-ch:
			if !ok {
				// Stream closed
				eofEvent := map[string]interface{}{"type": "eof"}
				if jsonBytes, err := json.Marshal(eofEvent); err == nil {
					fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
					flusher.Flush()
				}
				return
			}
			dataEvent := map[string]interface{}{
				"type":    "data",
				"payload": string(data),
			}
			if jsonBytes, err := json.Marshal(dataEvent); err == nil {
				fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func findFreePort() (int, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port, nil
}

func GetLocalIP() string {
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", 200*time.Millisecond)
	if err == nil {
		defer conn.Close()
		return conn.LocalAddr().(*net.UDPAddr).IP.String()
	}

	// Fallback to searching local interfaces
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			ip4 := ip.To4()
			if ip4 != nil {
				return ip4.String()
			}
		}
	}

	return "127.0.0.1"
}

func guessMIME(name string) string {
	ext := filepath.Ext(name)
	mimes := map[string]string{
		".pdf":  "application/pdf",
		".zip":  "application/zip",
		".tar":  "application/x-tar",
		".gz":   "application/gzip",
		".mp4":  "video/mp4",
		".mkv":  "video/x-matroska",
		".mp3":  "audio/mpeg",
		".png":  "image/png",
		".jpg":  "image/jpeg",
		".jpeg": "image/jpeg",
		".gif":  "image/gif",
		".webp": "image/webp",
		".txt":  "text/plain",
		".md":   "text/markdown",
		".html": "text/html",
		".json": "application/json",
		".csv":  "text/csv",
		".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	}
	if m, ok := mimes[ext]; ok {
		return m
	}
	return "application/octet-stream"
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")

	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "multipart reader error: "+err.Error(), http.StatusBadRequest)
		return
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "read part error: "+err.Error(), http.StatusBadRequest)
			return
		}

		if part.FormName() == "file" {
			cleanBase := filepath.Base(filepath.Clean(part.FileName()))
			cleanBase = strings.Trim(cleanBase, "\x00./\\")
			if cleanBase == "" {
				cleanBase = "upload.bin"
			}
			outName := "received_" + cleanBase
			outFile, err := os.OpenFile(outName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				part.Close()
				http.Error(w, "create file error: "+err.Error(), http.StatusInternalServerError)
				return
			}

			buffer := make([]byte, 64*1024)
			totalReceived := int64(0)
			start := time.Now()

			for {
				n, errRead := part.Read(buffer)
				if n > 0 {
					_, errWrite := outFile.Write(buffer[:n])
					if errWrite != nil {
						outFile.Close()
						part.Close()
						http.Error(w, "write file error: "+errWrite.Error(), http.StatusInternalServerError)
						return
					}
					totalReceived += int64(n)
					
					if r.ContentLength > 0 {
						pct := float64(totalReceived) / float64(r.ContentLength) * 100
						fmt.Printf("\r  📥 Receiving HTTP Upload: %.1f%% (%s/%s)",
							pct,
							formatBytes(totalReceived),
							formatBytes(r.ContentLength),
						)
					} else {
						fmt.Printf("\r  📥 Receiving HTTP Upload: %s",
							formatBytes(totalReceived),
						)
					}
				}
				if errRead != nil {
					break
				}
			}
			outFile.Close()
			part.Close()

			elapsed := time.Since(start)
			speed := float64(0)
			if elapsed.Seconds() > 0 {
				speed = float64(totalReceived) / elapsed.Seconds()
			}
			fmt.Printf("\n  ✅ HTTP Upload complete! Received %s and saved to %s in %.1fs (avg %s/s)\n",
				formatBytes(totalReceived),
				outName,
				elapsed.Seconds(),
				formatBytes(int64(speed)),
			)

			s.UpdateSharedFile(outName, part.FileName(), totalReceived)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "filename": outName})
			return
		}
		part.Close()
	}

	http.Error(w, "no file part found", http.StatusBadRequest)
}

// formatBytes helper for server logging
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// UpdateSharedFile changes the server's current file to serve a newly uploaded file.
func (s *Server) UpdateSharedFile(filePath string, fileName string, fileSize int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.filePath = filePath
	s.fileName = fileName
	s.fileSize = fileSize
	s.isLivePipe = false
	s.liveFinished = true
	for _, ch := range s.liveClients {
		close(ch)
	}
	s.liveClients = nil
}

func (s *Server) handleQR(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	urlParam := r.URL.Query().Get("url")
	if urlParam == "" {
		http.Error(w, "missing url parameter", http.StatusBadRequest)
		return
	}

	pngBytes, err := qrcode.Encode(urlParam, qrcode.Medium, 256)
	if err != nil {
		http.Error(w, "failed to generate qr code: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(pngBytes)))
	w.Write(pngBytes)
}

// RingBuffer is a byte-based circular buffer used to store a fixed size of the most recent live stream data.
type RingBuffer struct {
	buf    []byte
	head   int // read index
	tail   int // write index
	isFull bool
}

const (
	MinRingBufferSize = 64 * 1024         // 64 KB
	MaxRingBufferSize = 100 * 1024 * 1024 // 100 MB
)

// NewRingBuffer creates a RingBuffer of the specified size, clamped within safe boundaries.
func NewRingBuffer(size int) *RingBuffer {
	if size < MinRingBufferSize {
		size = MinRingBufferSize
	}
	if size > MaxRingBufferSize {
		size = MaxRingBufferSize
	}
	return &RingBuffer{
		buf: make([]byte, size),
	}
}

// Write appends bytes to the ring buffer, overwriting the oldest data if necessary.
func (r *RingBuffer) Write(p []byte) (n int, err error) {
	if len(p) >= len(r.buf) {
		// Just keep the last len(r.buf) bytes
		p = p[len(p)-len(r.buf):]
		copy(r.buf, p)
		r.head = 0
		r.tail = 0
		r.isFull = true
		return len(p), nil
	}

	avail := len(r.buf) - r.tail
	if len(p) <= avail {
		copy(r.buf[r.tail:], p)
		r.tail += len(p)
		if r.tail == len(r.buf) {
			r.tail = 0
			r.isFull = true
		}
	} else {
		copy(r.buf[r.tail:], p[:avail])
		copy(r.buf, p[avail:])
		r.tail = len(p) - avail
		r.isFull = true
	}

	if r.isFull {
		r.head = r.tail
	}

	return len(p), nil
}

// Bytes returns the ordered contents of the buffer, optionally trimmed to start at the first newline.
func (r *RingBuffer) Bytes() []byte {
	if !r.isFull {
		if r.tail == 0 {
			return nil
		}
		res := make([]byte, r.tail)
		copy(res, r.buf[:r.tail])
		return res
	}

	res := make([]byte, len(r.buf))
	copy(res, r.buf[r.head:])
	copy(res[len(r.buf)-r.head:], r.buf[:r.head])
	
	// Trim to the first newline to avoid partial lines
	idx := -1
	for i := 0; i < len(res); i++ {
		if res[i] == '\n' {
			idx = i
			break
		}
	}
	if idx != -1 && idx < len(res)-1 {
		return res[idx+1:]
	}
	return res
}
