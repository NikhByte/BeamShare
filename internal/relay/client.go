package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// HTTPClient defines an abstract HTTP transport interface for relay operations.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
	Get(url string) (*http.Response, error)
	Post(url, contentType string, body io.Reader) (*http.Response, error)
}

type Client struct {
	Key []byte

	BaseURL   string
	SessionID string
	HTTP      HTTPClient
}

func NewClient(url string) *Client {
	return &Client{
		BaseURL: url,
		HTTP: &http.Client{
			Timeout: 0, // No global timeout for long polling/streaming
		},
	}
}

func ensureTimeout(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && defaultTimeout > 0 {
		return context.WithTimeout(ctx, defaultTimeout)
	}
	return ctx, func() {}
}

func (c *Client) Register(ctx context.Context) (string, error) {
	reqCtx, cancel := ensureTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.BaseURL+"/relay/register", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("relay register failed with status: %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	c.SessionID = result["session"]
	return c.SessionID, nil
}

func (c *Client) PushState(ctx context.Context, offer string, candidates []map[string]interface{}, meta map[string]interface{}) error {
	reqCtx, cancel := ensureTimeout(ctx, 10*time.Second)
	defer cancel()

	body := map[string]interface{}{
		"offer":      offer,
		"candidates": candidates,
		"meta":       meta,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, fmt.Sprintf("%s/relay/state?session=%s", c.BaseURL, c.SessionID), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("relay state failed with status: %d", resp.StatusCode)
	}
	return nil
}

type PollCommand struct {
	Action string `json:"action"`
	Answer string `json:"answer,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Range  string `json:"range,omitempty"`
}

func (c *Client) Poll(ctx context.Context) (*PollCommand, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/relay/poll?session=%s", c.BaseURL, c.SessionID), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay poll failed with status: %d", resp.StatusCode)
	}

	var cmd PollCommand
	if err := json.NewDecoder(resp.Body).Decode(&cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// UploadData streams file data starting at the beginning of the file.
func (c *Client) UploadData(ctx context.Context, filePath string) error {
	return c.UploadDataAtOffset(ctx, filePath, 0)
}

// UploadDataAtOffset streams file data starting at a specified byte offset (for resumable transfers).
func (c *Client) UploadDataAtOffset(ctx context.Context, filePath string, offset int64) error {
	if ctx == nil {
		ctx = context.Background()
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("seek file to offset %d: %w", offset, err)
		}
	}

	var r io.Reader = file
	if len(c.Key) == 32 {
		r, err = NewEncryptingReader(file, c.Key)
		if err != nil {
			return err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/relay/data?session=%s", c.BaseURL, c.SessionID), r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("relay upload failed with status: %d", resp.StatusCode)
	}
	return nil
}
