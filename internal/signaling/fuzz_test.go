package signaling

import (
	"encoding/json"
	"testing"

	"github.com/pion/webrtc/v3"
)

func FuzzDecompressSDP(f *testing.F) {
	// Seed corpus with realistic valid compressed SDP
	rawSDP := "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=sendrecv\r\n"
	compressed, err := compressSDP(rawSDP)
	if err == nil {
		f.Add(compressed)
	}
	f.Add("")
	f.Add("not-valid-base64!!!")
	f.Add("AAAA") // valid base64, invalid zlib
	f.Add("eJw=")
	f.Add(string(make([]byte, 1024)))

	f.Fuzz(func(t *testing.T, input string) {
		// Ensure DecompressSDP handles any input without crashing or panicking
		_, _ = DecompressSDP(input)
	})
}

func FuzzParseICEURL(f *testing.F) {
	// Seed corpus with valid and edge-case URLs
	f.Add("stun:stun.l.google.com:19302")
	f.Add("stun:stun1.l.google.com:19302")
	f.Add("stuns:stun.example.com:5349")
	f.Add("turn:turn.example.com:3478?transport=udp")
	f.Add("turns:turn.example.com:5349?transport=tcp")
	f.Add("stun:127.0.0.1:3478")
	f.Add("stun:[::1]:3478")
	f.Add("")
	f.Add("http://invalid.scheme.com")
	f.Add("stun:")
	f.Add("stun:host:notaport")
	f.Add("stun:host:-1")
	f.Add("turn:user:pass@host:3478")

	f.Fuzz(func(t *testing.T, rawURL string) {
		// Must not panic on any malformed input
		_, _, _ = ParseICEURL(rawURL)
		_ = CheckNAT(rawURL, "127.0.0.1")
	})
}

func FuzzSignalingAnswerJSON(f *testing.F) {
	// Seed corpus with valid answer JSON
	validDesc := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  "v=0\r\no=- 987654 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
	}
	validBytes, _ := json.Marshal(validDesc)
	f.Add(validBytes)
	f.Add([]byte("{}"))
	f.Add([]byte(`{"type": "answer", "sdp": ""}`))
	f.Add([]byte(`{"type": "offer", "sdp": 12345}`))
	f.Add([]byte(`[1, 2, 3]`))
	f.Add([]byte(`"plain string"`))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var desc webrtc.SessionDescription
		_ = json.Unmarshal(data, &desc)
	})
}
