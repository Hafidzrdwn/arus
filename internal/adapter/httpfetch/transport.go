package httpfetch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/proxy"
)

// ProxyMode defines how outbound proxy routing is resolved.
type ProxyMode string

const (
	// ProxyModeNone bypasses all proxies and connects directly.
	ProxyModeNone ProxyMode = "none"
	// ProxyModeSystem uses standard HTTP_PROXY / HTTPS_PROXY environment variables.
	ProxyModeSystem ProxyMode = "system"
	// ProxyModeManual directs requests via a user-specified HTTP or SOCKS5 proxy URL.
	ProxyModeManual ProxyMode = "manual"
)

// TransportConfig specifies connection pooling, timeouts, and proxy routing for HTTP fetching.
type TransportConfig struct {
	MaxConnsPerHost       int
	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
	DialTimeout           time.Duration
	KeepAlive             time.Duration
	ProxyMode             ProxyMode
	ProxyURL              string
}

// DefaultTransportConfig returns standard HTTP client network settings aligned with TECH_SPEC §6.2.
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		MaxConnsPerHost:       16,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialTimeout:           15 * time.Second,
		KeepAlive:             30 * time.Second,
		ProxyMode:             ProxyModeSystem,
	}
}

// NewTransport constructs an *http.Transport tailored for segmented downloads.
// Forces HTTP/1.1 (HTTP/2 disabled via non-nil empty TLSNextProto) and disables transparent gzip.
func NewTransport(cfg TransportConfig) (*http.Transport, error) {
	if cfg.MaxConnsPerHost <= 0 {
		cfg.MaxConnsPerHost = 16
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = cfg.MaxConnsPerHost
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = 60 * time.Second
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = 15 * time.Second
	}
	if cfg.ResponseHeaderTimeout <= 0 {
		cfg.ResponseHeaderTimeout = 30 * time.Second
	}
	if cfg.ExpectContinueTimeout <= 0 {
		cfg.ExpectContinueTimeout = 1 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 15 * time.Second
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}

	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlive,
	}

	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false,
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		DisableCompression:    true,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimeout,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	switch cfg.ProxyMode {
	case ProxyModeNone:
		tr.Proxy = nil
	case ProxyModeSystem:
		tr.Proxy = http.ProxyFromEnvironment
	case ProxyModeManual:
		if cfg.ProxyURL == "" {
			return nil, fmt.Errorf("manual proxy mode requires a non-empty ProxyURL")
		}
		u, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL %q: %w", cfg.ProxyURL, err)
		}
		switch u.Scheme {
		case "http", "https":
			tr.Proxy = http.ProxyURL(u)
		case "socks5", "socks5h":
			sDialer, err := proxy.FromURL(u, dialer)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize SOCKS5 dialer: %w", err)
			}
			if ctxDialer, ok := sDialer.(proxy.ContextDialer); ok {
				tr.DialContext = ctxDialer.DialContext
			} else {
				tr.DialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
					return sDialer.Dial(network, addr)
				}
			}
			tr.Proxy = nil
		default:
			return nil, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
		}
	default:
		tr.Proxy = http.ProxyFromEnvironment
	}

	return tr, nil
}
