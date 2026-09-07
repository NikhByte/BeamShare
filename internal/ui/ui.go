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
	return Colorize(color, text)
}

// Colorize applies ANSI color codes if the terminal is a TTY and NO_COLOR is not set.
func Colorize(color, text string) string {
	if os.Getenv("NO_COLOR") != "" || !IsTTY() {
		return text
	}
	return color + text + reset
}

// IsTTY reports whether standard output is connected to a character device (terminal).
func IsTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func isTTY() bool {
	return IsTTY()
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

	// Generate a QR code bitmap (low error correction to keep matrix small for large URLs).
	qr, err := qrcode.New(content, qrcode.Low)
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
	lines := RenderHalfBlockLines(bitmap)
	for _, line := range lines {
		fmt.Println(line)
	}
}

// RenderHalfBlockLines converts a QR boolean matrix into terminal half-block string lines.
func RenderHalfBlockLines(bitmap [][]bool) []string {
	numRows := len(bitmap)
	if numRows == 0 {
		return nil
	}
	indent := "  "
	var lines []string

	for row := 0; row < numRows; row += 2 {
		line := indent
		numCols := len(bitmap[row])
		for col := 0; col < numCols; col++ {
			top := bitmap[row][col]
			var bottom bool
			if row+1 < numRows && col < len(bitmap[row+1]) {
				bottom = bitmap[row+1][col]
			}
			line += HalfBlockChar(top, bottom)
		}
		line = strings.TrimRight(line, " ")
		lines = append(lines, line)
	}
	return lines
}

func halfBlockChar(top, bottom bool) string {
	return HalfBlockChar(top, bottom)
}

// HalfBlockChar maps two QR module values to a UTF-8 half-block character.
// QR convention: true = dark (black), false = light (white).
func HalfBlockChar(top, bottom bool) string {
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
	if b < 0 {
		return "0 B"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit && exp < 5; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// CalculateProgress safely computes transfer percentage (0.0 to 100.0) without division-by-zero panics.
func CalculateProgress(current, total int64) float64 {
	if total <= 0 {
		if current >= 0 {
			return 100.0
		}
		return 0.0
	}
	if current <= 0 {
		return 0.0
	}
	pct := (float64(current) / float64(total)) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return pct
}

// FormatPercentage returns a formatted percentage string e.g. "45.2%".
func FormatPercentage(current, total int64) string {
	return fmt.Sprintf("%.1f%%", CalculateProgress(current, total))
}

// CalculateSpeed returns transfer speed in bytes/sec safely guarding against zero or instant duration.
func CalculateSpeed(bytes int64, elapsed time.Duration) float64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0.0
	}
	sec := elapsed.Seconds()
	if sec <= 0 {
		return 0.0
	}
	return float64(bytes) / sec
}

// FormatSpeed returns a human-readable transfer speed e.g. "12.4 MB/s".
func FormatSpeed(bytes int64, elapsed time.Duration) string {
	speed := CalculateSpeed(bytes, elapsed)
	return fmt.Sprintf("%s/s", FormatBytes(int64(speed)))
}

// CalculateETA returns estimated remaining duration or 0 if speed is invalid.
func CalculateETA(remainingBytes int64, bytesPerSec float64) time.Duration {
	if remainingBytes <= 0 || bytesPerSec <= 0 {
		return 0
	}
	seconds := float64(remainingBytes) / bytesPerSec
	if seconds > float64(24*3600*365) { // Cap to 1 year
		return 365 * 24 * time.Hour
	}
	return time.Duration(seconds * float64(time.Second))
}

// FormatETA formats remaining time into human-readable MM:SS or HH:MM:SS format, or "--:--" when indeterminate.
func FormatETA(remainingBytes int64, bytesPerSec float64) string {
	d := CalculateETA(remainingBytes, bytesPerSec)
	if d <= 0 {
		return "--:--"
	}
	totalSeconds := int64(d.Seconds())
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if hours > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

// RenderProgressBar generates a visual terminal progress bar string of specified width e.g. "[██████░░░░░░]".
func RenderProgressBar(current, total int64, width int) string {
	if width < 5 {
		width = 20
	}
	pct := CalculateProgress(current, total)
	completedBlocks := int((pct / 100.0) * float64(width))
	if completedBlocks > width {
		completedBlocks = width
	}
	if completedBlocks < 0 {
		completedBlocks = 0
	}
	remainingBlocks := width - completedBlocks

	return "[" + strings.Repeat("█", completedBlocks) + strings.Repeat("░", remainingBlocks) + "]"
}
