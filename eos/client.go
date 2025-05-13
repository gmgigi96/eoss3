package eos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/user"
	"strconv"
	"strings"

	erpc "github.com/cern-eos/go-eosgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Auth holds the information of the authenticated user.
// It's used to impersonate the user making the request
// over EOS.
type Auth struct {
	// Uid is the user id of the user.
	Uid uint64
	// Gid is the group id of the user.
	Gid uint64
}

// Username returns the username associated with the uid.
func (a *Auth) Username() string {
	u, err := user.LookupId(strconv.FormatUint(a.Uid, 10))
	if err != nil {
		return "<unknown>"
	}
	return u.Username
}

// Client represents a client for EOS.
type Client struct {
	conn       *grpc.ClientConn
	grpcClient erpc.EosClient
	httpClient *http.Client

	httpUrl string
	authKey string
}

// Config holds the configuration used by the EOS client.
type Config struct {
	// GrpcURL is the URL of the GRPC server.
	GrpcURL string
	// HttpURL is the URL of the HTTP server.
	HttpURL string
	// AuthKey is the key that authorizes the client to the HTTP/GRPC servers.
	AuthKey string
}

// Validate returns nil if the configuration is valid,
// meaning that all the mandatory configuration parameters
// have been set. Returns an error otherwise.
func (c *Config) Validate() error {
	if c.HttpURL == "" {
		return errors.New("missing http url")
	}
	if _, err := url.Parse(c.HttpURL); err != nil {
		return fmt.Errorf("error parsing http url: %w", err)
	}

	if c.GrpcURL == "" {
		return errors.New("missing grpc url")
	}
	if _, err := url.Parse(c.GrpcURL); err != nil {
		return fmt.Errorf("error parsing grpc url: %w", err)
	}

	if c.AuthKey == "" {
		return errors.New("missing authorization key")
	}

	return nil
}

// NewClient return a client for EOS.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	conn, err := grpc.NewClient(cfg.GrpcURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error getting grpc client: %w", err)
	}
	grpcClient := erpc.NewEosClient(conn)

	client := &Client{
		conn:       conn,
		grpcClient: grpcClient,
		httpClient: httpClient,
		httpUrl:    cfg.HttpURL,
		authKey:    cfg.AuthKey,
	}

	return client, nil
}

func (c *Client) Stat(ctx context.Context, auth Auth, path string) (*erpc.MDResponse, error) {
	req := &erpc.MDRequest{
		Type: erpc.TYPE_STAT,
		Id: &erpc.MDId{
			Path: []byte(path),
		},
		Authkey: c.authKey,
		Role: &erpc.RoleId{
			Uid: auth.Uid,
			Gid: auth.Gid,
		},
	}
	res, err := c.grpcClient.MD(ctx, req)
	if err != nil {
		return nil, err
	}

	r, err := res.Recv()
	if err != nil {
		return nil, ErrNoSuchResource{Path: path}
	}
	return r, nil
}

type ListDirFilters struct {
	Prefix *string
}

func (c *Client) ListDir(ctx context.Context, auth Auth, dir string, f func(*erpc.MDResponse), filters *ListDirFilters) error {
	req := &erpc.FindRequest{
		Type: erpc.TYPE_LISTING,
		Id: &erpc.MDId{
			Path: []byte(dir),
		},
		Role: &erpc.RoleId{
			Uid: auth.Uid,
			Gid: auth.Gid,
		},
		Authkey:  c.authKey,
		Maxdepth: 1,
	}

	// Add further filters to the find request based on user input
	if filters != nil {
		req.Selection = &erpc.MDSelection{
			Select: true,
		}
		if filters.Prefix != nil {
			req.Selection.RegexpFilename = []byte("^" + *filters.Prefix)
		}
	}

	res, err := c.grpcClient.Find(ctx, req)
	if err != nil {
		return err
	}

	i := 0
	for {
		r, err := res.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		i++
		if i == 1 {
			// The first received entry is the directory itseld
			// We then skip it
			continue
		}

		f(r)
	}
}

func (c *Client) Mkdir(ctx context.Context, auth Auth, path string, mode int64) error {
	req := c.initNsRequest(auth)
	req.Command = &erpc.NSRequest_Mkdir{
		Mkdir: &erpc.NSRequest_MkdirRequest{
			Id: &erpc.MDId{
				Path: []byte(path),
			},
			Recursive: true,
			Mode:      mode,
		},
	}

	res, err := c.grpcClient.Exec(ctx, req)
	if err != nil {
		return err
	}

	if res.Error != nil && res.Error.Code != 0 {
		return errors.New(res.Error.Msg)
	}

	return nil
}

func (c *Client) initNsRequest(auth Auth) *erpc.NSRequest {
	return &erpc.NSRequest{
		Role: &erpc.RoleId{
			Uid: auth.Uid,
			Gid: auth.Gid,
		},
		Authkey: c.authKey,
	}
}

func (c *Client) Rmdir(ctx context.Context, auth Auth, path string) error {
	req := c.initNsRequest(auth)
	req.Command = &erpc.NSRequest_Rmdir{
		Rmdir: &erpc.NSRequest_RmdirRequest{
			Id: &erpc.MDId{
				Path: []byte(path),
			},
		},
	}

	res, err := c.grpcClient.Exec(ctx, req)
	if err != nil {
		return err
	}

	if res.Error != nil && res.Error.Code != 0 {
		return errors.New(res.Error.Msg)
	}

	return nil
}

func (c *Client) Remove(ctx context.Context, auth Auth, path string) error {
	req := c.initNsRequest(auth)
	req.Command = &erpc.NSRequest_Rm{
		Rm: &erpc.NSRequest_RmRequest{
			Id: &erpc.MDId{
				Path: []byte(path),
			},
		},
	}
	res, err := c.grpcClient.Exec(ctx, req)
	if err != nil {
		return err
	}

	if res.Error.Code != 0 {
		return errors.New(res.Error.Msg)
	}

	return nil
}

func (c *Client) buildFullHttpUrl(auth Auth, path string) string {
	fullurl := strings.TrimRight(c.httpUrl, "/")
	fullurl += "/"
	fullurl += strings.TrimLeft(path, "/")

	fullurl += fmt.Sprintf("?eos.ruid=%d&eos.rgid=%d", auth.Uid, auth.Gid)

	final := strings.ReplaceAll(fullurl, "#", "%23")
	return final
}

func (c *Client) Download(ctx context.Context, auth Auth, path string) (io.ReadCloser, int64, error) {
	url := c.buildFullHttpUrl(auth, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	for {
		req.Header.Set("x-gateway-authorization", c.authKey)
		req.Header.Set("x-forwarded-for", "dummy") // TODO: is this really neaded??
		req.Header.Set("remote-user", auth.Username())

		res, err := c.httpClient.Do(req)
		if err != nil {
			return nil, 0, err
		}

		if res.StatusCode == http.StatusFound || res.StatusCode == http.StatusTemporaryRedirect {
			// we got redirected

			loc, err := res.Location()
			if err != nil {
				return nil, 0, err
			}

			req, err = http.NewRequestWithContext(ctx, http.MethodGet, loc.String(), nil)
			if err != nil {
				return nil, 0, err
			}
			continue
		}

		if res.StatusCode != http.StatusOK {
			return nil, 0, fmt.Errorf("got non OK status code from %s: %d", req.URL.String(), res.StatusCode)
		}

		return res.Body, res.ContentLength, nil
	}
}

func (c *Client) Upload(ctx context.Context, auth Auth, path string, data io.Reader, length uint64) error {
	url := c.buildFullHttpUrl(auth, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, nil)
	if err != nil {
		return err
	}

	for {
		req.Header.Set("x-gateway-authorization", c.authKey)
		req.Header.Set("x-forwarded-for", "dummy") // TODO: is this really neaded??
		req.Header.Set("remote-user", auth.Username())

		fmt.Println("doing request to", req.URL.String())

		res, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}

		if res.StatusCode == http.StatusTemporaryRedirect {
			// we got redirected to an FST

			loc, err := res.Location()
			if err != nil {
				return err
			}

			req, err = http.NewRequestWithContext(ctx, http.MethodPut, loc.String(), data)
			if err != nil {
				return err
			}
			req.Header.Set("Content-Length", strconv.FormatUint(length, 10))
			continue
		}

		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
			return fmt.Errorf("got non OK status code from %s: %d", req.URL.String(), res.StatusCode)
		}

		return nil
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
