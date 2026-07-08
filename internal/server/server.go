// Package server implements the local HTTP server that serves the Gaze
// receiver UI, the file-download API, and (Phase 3+) the WebRTC signaling API.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	isLivePipe   bool
	liveData     []byte
	liveClients  []chan []byte
	liveFinished bool
}

// FileMeta is the JSON response for /api/meta.
type FileMeta struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	MIME string `json:"mime"`
}

// New creates and configures a new Server. If filePath is empty,
// it runs in Live Pipe mode.
func New(filePath string) (*Server, error) {
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
		filePath:   filePath,
		fileName:   fileName,
		fileSize:   fileSize,
		port:       port,
		mux:        mux,
		isLivePipe: isLive,
	}

	// Core routes.
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/sw.js", s.handleServiceWorker)
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

	s.liveData = append(s.liveData, data...)

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
	data := make([]byte, len(s.liveData))
	copy(data, s.liveData)
	return data
}

// Mux returns the underlying ServeMux.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// LocalURL returns the http://lan-ip:port URL for this session.
func (s *Server) LocalURL() string {
	return fmt.Sprintf("http://%s:%d", localIP(), s.port)
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
	w.Write([]byte(assets.IndexHTML()))
}

func (s *Server) handleServiceWorker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Service-Worker-Allowed", "/")
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
	s.mu.Lock()
	s.downloads++
	count := s.downloads
	s.mu.Unlock()
	fmt.Printf("\r  Receiver connected (download #%d)…\n", count)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Disposition")

	if s.isLivePipe {
		s.mu.Lock()
		data := make([]byte, len(s.liveData))
		copy(data, s.liveData)
		s.mu.Unlock()

		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, s.fileName))
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Write(data)
		return
	}

	f, err := os.Open(s.filePath)
	if err != nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, s.fileName))
	w.Header().Set("Content-Type", guessMIME(s.fileName))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", s.fileSize))

	http.ServeContent(w, r, s.fileName, time.Now(), f)
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

	ch := make(chan []byte, 100)

	s.mu.Lock()
	// Send backlog first
	if len(s.liveData) > 0 {
		backlogEvent := map[string]interface{}{
			"type":    "backlog",
			"payload": string(s.liveData),
		}
		if jsonBytes, err := json.Marshal(backlogEvent); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
			flusher.Flush()
		}
	}

	if s.liveFinished {
		s.mu.Unlock()
		eofEvent := map[string]interface{}{"type": "eof"}
		if jsonBytes, err := json.Marshal(eofEvent); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
			flusher.Flush()
		}
		return
	}

	s.liveClients = append(s.liveClients, ch)
	s.mu.Unlock()

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

func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
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
			outName := "received_" + part.FileName()
			outFile, err := os.Create(outName)
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
			speed := float64(totalReceived) / elapsed.Seconds()
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
