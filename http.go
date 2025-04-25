package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type EOSHTTPClient struct {
	cl  *http.Client
	cfg *HTTPConfig
}

type HTTPConfig struct {
	// URL of the EOS HTTP server
	ServerURL string
	// Authkey is the key that authorizes this client to the EOS HTTP server
	Authkey string
	// Uid is the user id doing the HTTP request
	Uid int
	// Gid is the group id doing the HTTP request
	Gid int
	// Username which is doing the HTTP request
	Username string
}

func (c *HTTPConfig) Validate() error {
	if c.ServerURL == "" {
		return errors.New("ServerURL is empty")
	}

	if c.Authkey == "" {
		return errors.New("Authkey is empty")
	}

	return nil
}

func NewEOSHTTPClient(cfg *HTTPConfig) (*EOSHTTPClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cl := &http.Client{}

	eosClient := EOSHTTPClient{
		cl:  cl,
		cfg: cfg,
	}
	return &eosClient, nil
}

// Get download a file. The file can be read from the stream the function is returning
// It's responsibility of the user to close the stream once it has been read.
func (c *EOSHTTPClient) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	url := c.buildFullURL(path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	for {
		req.Header.Set("x-gateway-authorization", c.cfg.Authkey)
		req.Header.Set("x-forwarded-for", "dummy") // TODO: is this really neaded??
		req.Header.Set("remote-user", c.cfg.Username)

		res, err := c.cl.Do(req)
		if err != nil {
			return nil, err
		}

		if res.StatusCode == http.StatusFound || res.StatusCode == http.StatusTemporaryRedirect {
			// we got redirected

			loc, err := res.Location()
			if err != nil {
				return nil, err
			}

			req, err = http.NewRequestWithContext(ctx, http.MethodGet, loc.String(), nil)
			if err != nil {
				return nil, err
			}
			continue
		}

		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("got non OK status code from %s: %d", req.URL.String(), res.StatusCode)
		}

		return res.Body, nil
	}
}

func (c *EOSHTTPClient) buildFullURL(path string) string {
	fullurl := strings.TrimRight(c.cfg.ServerURL, "/")
	fullurl += "/"
	fullurl += strings.TrimLeft(path, "/")

	fullurl += fmt.Sprintf("?eos.ruid=%d&eos.rgid=%d", c.cfg.Uid, c.cfg.Gid)

	final := strings.ReplaceAll(fullurl, "#", "%23")
	return final
}
