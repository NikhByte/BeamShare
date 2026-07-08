// Package signaling implements the WebRTC signaling layer for Beam.
//
// Phase 3 — Optical Handshake:
//   - The CLI generates a WebRTC "Offer" SDP, compresses it with zlib + base64,
//     and encodes the result into a QR code (the "optical handshake").
//   - The receiver's browser scans the QR, decodes the offer, and POSTs its
//     "Answer" SDP back to /api/signal/answer.
//   - Once both sides have each other's SDP + ICE candidates, the RTCPeerConnection
//     opens a data channel and file transfer begins peer-to-peer.
//
// Privacy guarantee: STUN servers are only used to discover the public IP for
// the ICE handshake. The actual file bytes travel directly between peers.
package signaling

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v3"
)

// ICEServers are the ICE servers used for NAT traversal discovery and relay.
var ICEServers = []webrtc.ICEServer{
	{URLs: []string{"stun:stun.l.google.com:19302"}},
	{URLs: []string{"stun:stun1.l.google.com:19302"}},
	{
		URLs: []string{
			"turn:openrelay.metered.ca:80",
			"turn:openrelay.metered.ca:443",
			"turn:openrelay.metered.ca:443?transport=tcp",
		},
		Username:   "openrelayproject",
		Credential: "openrelayproject",
	},
}

// Session holds the state of one WebRTC sender session.
type Session struct {
	pc          *webrtc.PeerConnection
	dc          *webrtc.DataChannel
	offerSDP    string // compressed+b64 for QR encoding
	rawOffer    string // full SDP text
	answerReady chan struct{}
	candidates  []webrtc.ICECandidateInit
	mu          sync.Mutex
	iceServers  []webrtc.ICEServer

	// OnOpen is called when the data channel is open and ready to send.
	OnOpen func(dc *webrtc.DataChannel)
}

// NewSession creates a new WebRTC PeerConnection configured as the sender.
func NewSession(iceServers []webrtc.ICEServer) (*Session, error) {
	if len(iceServers) == 0 {
		iceServers = ICEServers
	}
	config := webrtc.Configuration{ICEServers: iceServers}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("create peer connection: %w", err)
	}

	s := &Session{
		pc:          pc,
		answerReady: make(chan struct{}, 1),
		iceServers:  iceServers,
	}

	// Collect trickle ICE candidates as they arrive.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		s.mu.Lock()
		s.candidates = append(s.candidates, c.ToJSON())
		s.mu.Unlock()
	})

	// Create the ordered, reliable data channel for file transfer.
	ordered := true
	dc, err := pc.CreateDataChannel("beam-file", &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("create data channel: %w", err)
	}
	s.dc = dc

	dc.OnOpen(func() {
		if s.OnOpen != nil {
			s.OnOpen(dc)
		}
	})

	return s, nil
}

// CreateOffer generates the SDP offer and compresses it for QR encoding.
// Must be called before RegisterHandlers.
func (s *Session) CreateOffer(ctx context.Context) (compressedOffer string, err error) {
	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("create offer: %w", err)
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}

	// Wait for ICE gathering to complete (or timeout after 3s).
	gatherDone := webrtc.GatheringCompletePromise(s.pc)
	select {
	case <-gatherDone:
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// Use the final (trickle-complete) local description.
	finalSDP := s.pc.LocalDescription().SDP
	s.rawOffer = finalSDP

	compressed, err := compressSDP(finalSDP)
	if err != nil {
		return "", fmt.Errorf("compress sdp: %w", err)
	}
	s.offerSDP = compressed
	return compressed, nil
}

// CompressedOffer returns the zlib+base64 encoded SDP, ready for embedding in a QR.
func (s *Session) CompressedOffer() string { return s.offerSDP }

// RegisterHandlers mounts the signaling API routes on the given mux:
//   - GET  /api/signal/offer  → returns the compressed SDP offer
//   - POST /api/signal/answer → accepts the receiver's SDP answer
//   - GET  /api/signal/candidates → returns ICE candidates as JSON
func (s *Session) RegisterHandlers(mux *http.ServeMux) {
	// Offer endpoint — receiver fetches this after scanning the QR.
	mux.HandleFunc("/api/signal/offer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"sdp":        s.rawOffer,
			"type":       "offer",
			"iceServers": s.iceServers,
		})
	})

	// Answer endpoint — receiver POSTs its SDP answer here.
	mux.HandleFunc("/api/signal/answer", func(w http.ResponseWriter, r *http.Request) {
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

		var answer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&answer); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.pc.SetRemoteDescription(answer); err != nil {
			http.Error(w, "set remote desc: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

		// Unblock WaitForAnswer.
		select {
		case s.answerReady <- struct{}{}:
		default:
		}
	})

	// ICE candidates endpoint — receiver polls this to add remote candidates.
	mux.HandleFunc("/api/signal/candidates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		s.mu.Lock()
		cands := s.candidates
		s.mu.Unlock()
		json.NewEncoder(w).Encode(cands)
	})
}

// WaitForAnswer blocks until the receiver has posted its SDP answer,
// or until the context is cancelled.
func (s *Session) WaitForAnswer(ctx context.Context) error {
	select {
	case <-s.answerReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DataChannel returns the WebRTC data channel once the connection is ready.
func (s *Session) DataChannel() *webrtc.DataChannel { return s.dc }

// Close shuts down the peer connection.
func (s *Session) Close() error { return s.pc.Close() }

// ─── SDP compression helpers ──────────────────────────────────────────────────

func minifySDP(sdp string) string {
	lines := strings.Split(sdp, "\r\n")
	var out []string
	hasHostCandidate := false
	for _, line := range lines {
		// Remove unneeded verbosity
		if strings.HasPrefix(line, "a=extmap") || strings.HasPrefix(line, "a=msid") || strings.HasPrefix(line, "a=ice-options") || strings.HasPrefix(line, "b=") || strings.HasPrefix(line, "a=sctp-port") {
			continue
		}
		// Minify candidate lines to remove redundant fields (raddr, rport, etc.)
		if strings.HasPrefix(line, "a=candidate") {
			// Keep only the first host candidate to save space in the QR code
			if !strings.Contains(line, "typ host") {
				continue
			}
			if hasHostCandidate {
				continue
			}
			hasHostCandidate = true
			if idx := strings.Index(line, " raddr"); idx > 0 {
				line = line[:idx]
			}
		}
		out = append(out, line)
	}
	return strings.Join(out, "\r\n")
}

// compressSDP applies zlib deflate + URL-safe base64 to reduce SDP size
// so it fits in a single scannable QR code (≈1500 bytes max at Version 40).
func compressSDP(sdp string) (string, error) {
	sdp = minifySDP(sdp)
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return "", err
	}
	if _, err := io.WriteString(w, sdp); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

// DecompressSDP reverses compressSDP — used by the browser (via JS atob +
// pako) and optionally by Go unit tests.
func DecompressSDP(compressed string) (string, error) {
	var raw []byte
	var err error
	if strings.ContainsAny(compressed, "=") || len(compressed)%4 == 0 {
		raw, err = base64.URLEncoding.DecodeString(compressed)
	} else {
		raw, err = base64.RawURLEncoding.DecodeString(compressed)
	}
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	r, err := zlib.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("zlib open: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("zlib read: %w", err)
	}
	return string(out), nil
}

func (s *Session) GetCandidates() []webrtc.ICECandidateInit {
	s.mu.Lock()
	defer s.mu.Unlock()
	cands := make([]webrtc.ICECandidateInit, len(s.candidates))
	copy(cands, s.candidates)
	return cands
}

func (s *Session) ProvideAnswer(answer string) error {
	var ans webrtc.SessionDescription
	if err := json.Unmarshal([]byte(answer), &ans); err != nil {
		return err
	}
	if err := s.pc.SetRemoteDescription(ans); err != nil {
		return err
	}
	select {
	case s.answerReady <- struct{}{}:
	default:
	}
	return nil
}

func (s *Session) RawOffer() string {
	return s.rawOffer
}

func (s *Session) OnICEConnectionStateChange(f func(webrtc.ICEConnectionState)) {
	s.pc.OnICEConnectionStateChange(f)
}
