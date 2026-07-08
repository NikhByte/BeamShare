package relay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Client struct {
	Key []byte

	BaseURL   string
	SessionID string
	HTTP      *http.Client
}

func NewClient(url string) *Client {
	return &Client{
		BaseURL: url,
		HTTP: &http.Client{
			Timeout: 0, // No timeout for long polling/streaming
		},
	}
}

func (c *Client) Register() (string, error) {
	resp, err := c.HTTP.Post(c.BaseURL+"/relay/register", "application/json", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	c.SessionID = result["session"]
	return c.SessionID, nil
}

func (c *Client) PushState(offer string, candidates []map[string]interface{}, meta map[string]interface{}) error {
	body := map[string]interface{}{
		"offer":      offer,
		"candidates": candidates,
		"meta":       meta,
	}
	b, _ := json.Marshal(body)
	resp, err := c.HTTP.Post(fmt.Sprintf("%s/relay/state?session=%s", c.BaseURL, c.SessionID), "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

type PollCommand struct {
	Action string `json:"action"`
	Answer string `json:"answer"`
}

func (c *Client) Poll() (*PollCommand, error) {
	// Long poll
	resp, err := c.HTTP.Get(fmt.Sprintf("%s/relay/poll?session=%s", c.BaseURL, c.SessionID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var cmd PollCommand
	if err := json.NewDecoder(resp.Body).Decode(&cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func (c *Client) UploadData(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var r io.Reader = file
	if len(c.Key) == 32 {
		r, err = NewEncryptingReader(file, c.Key)
		if err != nil {
			return err
		}
	}

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/relay/data?session=%s", c.BaseURL, c.SessionID), r)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
