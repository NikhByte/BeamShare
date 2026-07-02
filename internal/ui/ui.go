// Package ui handles all terminal output for the beam CLI:
// the startup banner, file metadata, live URL, QR code, and status lines.
//
// Phase 2 upgrade: QR codes are now generated via github.com/skip2/go-qrcode
// for guaranteed scannability. The image is rendered as UTF-8 half-block art
// (▀ / ▄ / █ / space) so each terminal cell represents two QR rows.
package ui

import (
	"fmt"
	"os"
	"strings"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

// ANSI colour helpers — gracefully degrade in environments without colour.
const (
	reset   = "\033[0m"
	bold    = "\033[1m"
	dim     = "\033[2m"
	cyan    = "\033[36m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	white   = "\033[97m"
)

func colorize(color, text string) string {
	if !isTTY() {
		return text
	}
	return color + text + reset
}

func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// PrintBanner renders the BEAM ASCII logo.
func PrintBanner() {
	lines := []string{
		"",
		"  ██████╗ ███████╗ █████╗ ███╗   ███╗",
		"  ██╔══██╗██╔════╝██╔══██╗████╗ ████║",
		"  ██████╔╝█████╗  ███████║██╔████╔██║",
		"  ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║",
		"  ██████╔╝███████╗██║  ██║██║ ╚═╝ ██║",
		"  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝",
		"",
		colorize(dim+white, "  Unstoppable P2P file transfer — even on restricted Wi-Fi"),
		"",
	}
	for _, l := range lines {
		fmt.Println(colorize(cyan, l))
	}
}

// PrintFileMeta shows the file name and formatted size.
func PrintFileMeta(name string, size int64) {
	bar := strings.Repeat("─", 44)
	fmt.Printf("  %s\n", colorize(dim, bar))
	fmt.Printf("  %s  %s\n",
		colorize(bold+white, "File :"),
		colorize(green, name),
	)
	fmt.Printf("  %s  %s\n",
		colorize(bold+white, "Size :"),
		colorize(yellow, FormatBytes(size)),
	)
	fmt.Printf("  %s\n\n", colorize(dim, bar))
}

// PrintDiscovery shows the mDNS hostname alongside the numeric IP URL.
func PrintDiscovery(localURL, mdnsName string) {
	fmt.Printf("  %s\n", colorize(bold+white, "Open on the receiver device:"))
	fmt.Printf("\n  %s  %s\n",
		colorize(bold+cyan, "  "+localURL+"  "),
		colorize(dim, "(numeric IP)"),
	)
	if mdnsName != "" {
		host := strings.TrimPrefix(localURL, "http://")
		// Replace the IP:port with mDNS name, keeping the port.
		parts := strings.SplitN(host, ":", 2)
		mdnsURL := "http://" + mdnsName
		if len(parts) == 2 {
			mdnsURL += ":" + parts[1]
		}
		fmt.Printf("  %s  %s\n",
			colorize(bold+green, "  "+mdnsURL+"  "),
			colorize(dim, "(mDNS — works without typing IP)"),
		)
	}
	fmt.Println()
}

// PrintReceiverURL shows the clickable URL the receiver must open.
// Kept for backwards-compatibility; Phase 2 callers use PrintDiscovery.
func PrintReceiverURL(url string) {
	PrintDiscovery(url, "")
}

// PrintQR renders a proper, scannable QR code for the given URL using the
// go-qrcode library. The image is drawn with UTF-8 half-block characters
// so each terminal cell represents two pixel rows.
func PrintQR(content string) {
	fmt.Printf("  %s\n\n", colorize(bold+white, "Or scan the QR code:"))

	// Generate a QR code bitmap (medium error correction is enough for URLs).
	qr, err := qrcode.New(content, qrcode.Medium)
	if err != nil {
		fmt.Printf("  (QR generation failed: %v)\n\n", err)
		return
	}

	// Get the raw boolean matrix (true = dark module).
	// The Bitmap() method returns the full image including quiet zone.
	bitmap := qr.Bitmap()
	renderHalfBlock(bitmap)
	fmt.Println()
}

// renderHalfBlock converts a QR bool matrix into terminal half-block art.
// Each pair of rows is merged into one line using ▀ / ▄ / █ / space.
func renderHalfBlock(bitmap [][]bool) {
	size := len(bitmap)
	indent := "    "

	for row := 0; row < size; row += 2 {
		line := indent
		for col := 0; col < size; col++ {
			top := bitmap[row][col]
			var bottom bool
			if row+1 < size {
				bottom = bitmap[row+1][col]
			}
			line += halfBlockChar(top, bottom)
		}
		line = strings.TrimRight(line, " ")
		fmt.Println(line)
	}
}

// halfBlockChar maps two QR module values to a UTF-8 half-block character.
// QR convention: true = dark (black), false = light (white).
func halfBlockChar(top, bottom bool) string {
	switch {
	case top && bottom:
		return "█"
	case top && !bottom:
		return "▀"
	case !top && bottom:
		return "▄"
	default:
		return " "
	}
}

// PrintWaiting shows the "waiting for receiver" status line.
func PrintWaiting(mdnsActive bool) {
	if mdnsActive {
		fmt.Printf("  %s\n",
			colorize(green, "  mDNS active — device reachable at .local address above"))
	}
	fmt.Printf("  %s\n", colorize(dim, "Waiting for receiver… (press Ctrl+C to cancel)"))
	fmt.Printf("  %s %s\n\n",
		colorize(dim, "Started at:"),
		colorize(dim+white, time.Now().Format("15:04:05")),
	)
}

// PrintMDNSError prints a non-fatal warning when mDNS fails to start.
func PrintMDNSError(err error) {
	fmt.Printf("  %s mDNS unavailable (%v) — use the IP URL instead\n\n",
		colorize(yellow, "  warn:"), err)
}

// ─── formatting ──────────────────────────────────────────────────────────────

// FormatBytes converts a byte count to a human-readable string.
func FormatBytes(b int64) string {
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
