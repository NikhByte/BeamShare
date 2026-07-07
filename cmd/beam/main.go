package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"strconv"
	"strings"

	"github.com/beamshare/beam/internal/mdns"
	"github.com/beamshare/beam/internal/server"
	"github.com/beamshare/beam/internal/signaling"
	"github.com/beamshare/beam/internal/ui"
	"github.com/pion/webrtc/v3"
)

const version = "0.4.0"

var (
	activeChannels []*webrtc.DataChannel
	channelsMu     sync.Mutex
	liveFinished   bool
)

func main() {
	fi, err := os.Stdin.Stat()
	isPipe := err == nil && (fi.Mode()&os.ModeCharDevice) == 0

	if len(os.Args) < 2 {
		if isPipe {
			runSend("")
			os.Exit(0)
		}
		printHelp()
		os.Exit(0)
	}

	cmd := os.Args[1]
	switch cmd {
	case "send":
		if len(os.Args) < 3 {
			if isPipe {
				runSend("")
				os.Exit(0)
			}
			fmt.Fprintln(os.Stderr, "beam: 'send' requires a file path or piped stdin")
			os.Exit(1)
		}
		runSend(os.Args[2])
	case "version", "--version", "-v":
		fmt.Printf("beam version %s\n", version)
	case "help", "--help", "-h":
		printHelp()
	default:
		if _, err := os.Stat(cmd); err == nil {
			runSend(cmd)
		} else {
			if isPipe {
				runSend("")
				os.Exit(0)
			}
			fmt.Fprintf(os.Stderr, "beam: unknown command/file '%s'\nRun 'beam help' for usage.\n", cmd)
			os.Exit(1)
		}
	}
}

func runSend(filePath string) {
	var isLive bool
	var fileName string
	var fileSize int64
	var fileInfo os.FileInfo

	if filePath == "" {
		isLive = true
		fileName = "stdin.txt"
		fileSize = -1
	} else {
		absPath, err := filepath.Abs(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "beam: %v\n", err)
			os.Exit(1)
		}
		fileInfo, err = os.Stat(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "beam: file not found: %s\n", absPath)
			os.Exit(1)
		}
		fileName = fileInfo.Name()
		fileSize = fileInfo.Size()
		filePath = absPath
	}

	// ── Banner + file metadata ────────────────────────────────────────────────
	ui.PrintBanner()
	ui.PrintFileMeta(fileName, fileSize)

	// ── HTTP server ───────────────────────────────────────────────────────────
	srv, err := server.New(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "beam: %v\n", err)
		os.Exit(1)
	}

	// ── Phase 3: WebRTC signaling session ─────────────────────────────────────
	fmt.Printf("  %s\n", dimStr("Setting up WebRTC session…"))
	session, err := signaling.NewSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "  warn: WebRTC unavailable (%v) — HTTP-only mode\n", err)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, offerErr := session.CreateOffer(ctx)
		if offerErr != nil {
			fmt.Fprintf(os.Stderr, "  warn: WebRTC offer failed (%v)\n", offerErr)
			session.Close()
			session = nil
		} else {
			// Register /api/signal/* routes on the server's mux.
			session.RegisterHandlers(srv.Mux())

			// Hook up data channel handler
			session.OnOpen = func(dc *webrtc.DataChannel) {
				// Upload state variables for incoming files from receiver
				var (
					uploadFile *os.File
					uploadName string
					uploadSize int64
					uploaded   int64
					uploadStat time.Time
				)

				dc.OnMessage(func(msg webrtc.DataChannelMessage) {
					if msg.IsString {
						dataStr := string(msg.Data)
						if strings.HasPrefix(dataStr, "UPLOAD_META:") {
							parts := strings.SplitN(dataStr, ":", 3)
							if len(parts) == 3 {
								name := parts[1]
								size, _ := strconv.ParseInt(parts[2], 10, 64)
								
								uploadName = "received_" + filepath.Base(name)
								uploadSize = size
								uploaded = 0
								uploadStat = time.Now()

								var errCreate error
								uploadFile, errCreate = os.Create(uploadName)
								if errCreate != nil {
									fmt.Printf("\n  Error creating upload file: %v\n", errCreate)
									uploadFile = nil
									return
								}
								fmt.Printf("\n  📥 Receiving P2P Upload: %s (%s)\n", uploadName, ui.FormatBytes(uploadSize))
							}
						} else if dataStr == "UPLOAD_EOF" {
							if uploadFile != nil {
								uploadFile.Close()
								elapsed := time.Since(uploadStat)
								speed := float64(uploaded) / elapsed.Seconds()
								fmt.Printf("\n  ✅ P2P Upload complete! Received %s and saved to %s in %.1fs (avg %s/s)\n",
									ui.FormatBytes(uploaded),
									uploadName,
									elapsed.Seconds(),
									ui.FormatBytes(int64(speed)),
								)
								srv.UpdateSharedFile(uploadName, filepath.Base(uploadName), uploaded)
								uploadFile = nil
							}
						}
					} else {
						if uploadFile != nil {
							n, errWrite := uploadFile.Write(msg.Data)
							if errWrite != nil {
								fmt.Printf("\n  Error writing upload chunk: %v\n", errWrite)
								uploadFile.Close()
								uploadFile = nil
								return
							}
							uploaded += int64(n)
							pct := float64(uploaded) / float64(uploadSize) * 100
							fmt.Printf("\r  📥 Receiving P2P: %.1f%% (%s/%s)",
								pct,
								ui.FormatBytes(uploaded),
								ui.FormatBytes(uploadSize),
							)
						}
					}
				})

				if isLive {
					channelsMu.Lock()
					if liveFinished {
						dc.SendText("EOF")
						dc.Close()
						channelsMu.Unlock()
						return
					}
					// Send backlog first so new connections see past piped input
					backlog := srv.GetLiveBacklog()
					if len(backlog) > 0 {
						dc.SendText(string(backlog))
					}
					activeChannels = append(activeChannels, dc)
					channelsMu.Unlock()
					fmt.Println("\n  [P2P] Receiver subscribed to live log stream!")
				} else {
					// File sender goroutine (Direct-to-Disk + Backpressure)
					go func() {
						fmt.Println("\n  [P2P] Direct P2P tunnel established! Streaming file...")
						file, err := os.Open(filePath)
						if err != nil {
							fmt.Printf("  Error opening file: %v\n", err)
							return
						}
						defer file.Close()

						// Send META header
						metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
						if errSend := dc.SendText(metaHeader); errSend != nil {
							fmt.Printf("  Error sending meta header: %v\n", errSend)
							return
						}

						buffer := make([]byte, 64*1024) // 64KB chunk size
						totalSent := int64(0)
						start := time.Now()

						for {
							// Backpressure check: if buffered amount > 1MB, wait
							if dc.BufferedAmount() > 1024*1024 {
								time.Sleep(10 * time.Millisecond)
								continue
							}

							n, err := file.Read(buffer)
							if n > 0 {
								errSend := dc.Send(buffer[:n])
								if errSend != nil {
									fmt.Printf("\n  Error sending chunk: %v\n", errSend)
									return
								}
								totalSent += int64(n)
								pct := float64(totalSent) / float64(fileSize) * 100
								fmt.Printf("\r  📤 Sending P2P: %.1f%% (%s/%s)",
									pct,
									ui.FormatBytes(totalSent),
									ui.FormatBytes(fileSize),
								)
							}
							if err != nil {
								break
							}
						}

						// Wait for buffer to clear before sending EOF
						for dc.BufferedAmount() > 0 {
							time.Sleep(10 * time.Millisecond)
						}
						dc.SendText("EOF")

						elapsed := time.Since(start)
						speed := float64(totalSent) / elapsed.Seconds()
						fmt.Printf("\n  ✅ P2P Transfer Complete! Sent %s in %.1fs (avg %s/s)\n",
							ui.FormatBytes(totalSent),
							elapsed.Seconds(),
							ui.FormatBytes(int64(speed)),
						)
					}()
				}
			}
		}
	}

	// ── Phase 5: Stdin reader for Live Pipe ──────────────────────────────────
	if isLive {
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := os.Stdin.Read(buf)
				if n > 0 {
					chunk := make([]byte, n)
					copy(chunk, buf[:n])

					// Print to local terminal stdout
					os.Stdout.Write(chunk)

					// Broadcast to HTTP SSE clients
					srv.WriteLive(chunk)

					// Broadcast to WebRTC data channels
					channelsMu.Lock()
					for _, dc := range activeChannels {
						dc.SendText(string(chunk))
					}
					channelsMu.Unlock()
				}
				if err != nil {
					// Stdin finished / EOF reached
					srv.CloseLive()

					channelsMu.Lock()
					liveFinished = true
					for _, dc := range activeChannels {
						dc.SendText("EOF")
						dc.Close()
					}
					activeChannels = nil
					channelsMu.Unlock()
					break
				}
			}
		}()
	}

	localURL := srv.LocalURL()

	// ── Phase 2: mDNS ────────────────────────────────────────────────────────
	broadcaster := mdns.New("", srv.Port())
	mdnsErr := broadcaster.Start()
	var mdnsName string
	if mdnsErr != nil {
		ui.PrintMDNSError(mdnsErr)
	} else {
		mdnsName = broadcaster.LocalName()
	}

	// ── Print URLs ────────────────────────────────────────────────────────────
	ui.PrintDiscovery(localURL, mdnsName)

	// ── Print QR ─────────────────────────────────────────────────────────────
	qrURL := localURL
	if session != nil {
		qrURL = localURL + "?mode=webrtc"
	}
	ui.PrintQR(qrURL)

	// ── Connection mode summary ───────────────────────────────────────────────
	if isLive {
		fmt.Printf("  Live Stream: %s\n", greenStr("active"))
	}
	if session != nil {
		fmt.Printf("  WebRTC P2P : %s\n", greenStr("ready"))
		fmt.Printf("  HTTP direct: %s\n", greenStr("ready  (fallback)"))
	} else {
		fmt.Printf("  HTTP direct: %s\n", greenStr("ready"))
	}
	fmt.Println()

	ui.PrintWaiting(mdnsErr == nil)

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("\n  beam: shutting down…")
		broadcaster.Stop()
		if session != nil {
			session.Close()
		}
		os.Exit(0)
	}()

	if err := srv.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "beam: server error: %v\n", err)
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Printf(`
  ██████╗ ███████╗ █████╗ ███╗   ███╗
  ██╔══██╗██╔════╝██╔══██╗████╗ ████║
  ██████╔╝█████╗  ███████║██╔████╔██║
  ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║
  ██████╔╝███████╗██║  ██║██║ ╚═╝ ██║
  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝

  Unstoppable P2P file transfer — even on restricted Wi-Fi.

  USAGE:
    beam send <file>     Share a file — prints URL + QR, opens WebRTC tunnel
    beam <file>          Shorthand for 'beam send <file>'
    beam [piped stdin]   Live Terminal Piping mode (Phase 5)
    beam version         Print version information
    beam help            Show this help message

  EXAMPLES:
    beam send report.pdf
    beam notes.txt
    cat logs.txt | beam   (Phase 5 — Live Pipe)

  HOW IT WORKS:
    Phase 1  Local HTTP server + embedded Gaze web UI
    Phase 2  mDNS broadcast (<hostname>.local) + real QR code
    Phase 3  WebRTC offer baked into QR → optical handshake → P2P tunnel
    Phase 4  64KB chunked streaming + Direct-to-Disk API (large files)
    Phase 5  Live terminal piping with local log visualizer

  Version: %s
`, version)
}

func dimStr(s string) string   { return "\033[2m" + s + "\033[0m" }
func greenStr(s string) string { return "\033[32m" + s + "\033[0m" }
