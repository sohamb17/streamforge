// Package dockerapi is a minimal client for the Docker Engine API over the
// local unix socket: just enough to list a compose project's containers,
// kill/start/pause/unpause them, and read CPU usage. The console uses it for
// the chaos buttons and the bench tool for CPU accounting.
package dockerapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to dockerd.
type Client struct {
	hc *http.Client
}

// New returns a client for the given socket path (usually
// /var/run/docker.sock).
func New(socket string) *Client {
	return &Client{hc: &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}},
	}}
}

func (c *Client) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("docker %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Container is a summary from /containers/json.
type Container struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	State  string            `json:"State"` // running, exited, paused, ...
	Status string            `json:"Status"`
	Labels map[string]string `json:"Labels"`
}

// Service returns the compose service name.
func (c Container) Service() string { return c.Labels["com.docker.compose.service"] }

// List returns all containers (running or not) of a compose project.
func (c *Client) List(ctx context.Context, project string) ([]Container, error) {
	f, _ := json.Marshal(map[string][]string{"label": {"com.docker.compose.project=" + project}})
	var out []Container
	err := c.do(ctx, http.MethodGet, "/containers/json?all=1&filters="+url.QueryEscape(string(f)), &out)
	return out, err
}

// Kill sends SIGKILL.
func (c *Client) Kill(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+id+"/kill", nil)
}

// Start starts a stopped container.
func (c *Client) Start(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+id+"/start", nil)
}

// Pause freezes all processes in the container.
func (c *Client) Pause(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+id+"/pause", nil)
}

// Unpause resumes a paused container.
func (c *Client) Unpause(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+id+"/unpause", nil)
}

// Stats is the subset of /containers/{id}/stats used for CPU accounting.
type Stats struct {
	CPU struct {
		Usage struct {
			Total uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		System uint64 `json:"system_cpu_usage"`
		Online int    `json:"online_cpus"`
	} `json:"cpu_stats"`
	Memory struct {
		Usage uint64 `json:"usage"`
	} `json:"memory_stats"`
}

// Stats returns a one-shot stats sample.
func (c *Client) Stats(ctx context.Context, id string) (Stats, error) {
	var s Stats
	err := c.do(ctx, http.MethodGet, "/containers/"+id+"/stats?stream=false&one-shot=true", &s)
	return s, err
}
