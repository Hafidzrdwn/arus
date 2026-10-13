package httpfetch_test

import (
	"arus/internal/adapter/httpfetch"
	"testing"
)

func TestNewTransport_HTTP1AndSettings(t *testing.T) {
	cfg := httpfetch.DefaultTransportConfig()
	tr, err := httpfetch.NewTransport(cfg)
	if err != nil {
		t.Fatalf("NewTransport error: %v", err)
	}

	if tr.ForceAttemptHTTP2 {
		t.Errorf("ForceAttemptHTTP2 = true, want false")
	}
	if tr.TLSNextProto == nil {
		t.Errorf("TLSNextProto is nil, want non-nil empty map to disable HTTP/2")
	}
	if len(tr.TLSNextProto) != 0 {
		t.Errorf("len(TLSNextProto) = %d, want 0", len(tr.TLSNextProto))
	}
	if !tr.DisableCompression {
		t.Errorf("DisableCompression = false, want true")
	}
	if tr.MaxConnsPerHost != 16 {
		t.Errorf("MaxConnsPerHost = %d, want 16", tr.MaxConnsPerHost)
	}
}

func TestNewTransport_ProxyModes(t *testing.T) {
	t.Run("None", func(t *testing.T) {
		cfg := httpfetch.DefaultTransportConfig()
		cfg.ProxyMode = httpfetch.ProxyModeNone
		tr, err := httpfetch.NewTransport(cfg)
		if err != nil {
			t.Fatalf("NewTransport error: %v", err)
		}
		if tr.Proxy != nil {
			t.Errorf("expected tr.Proxy to be nil for ProxyModeNone")
		}
	})

	t.Run("Manual_HTTP", func(t *testing.T) {
		cfg := httpfetch.DefaultTransportConfig()
		cfg.ProxyMode = httpfetch.ProxyModeManual
		cfg.ProxyURL = "http://127.0.0.1:8888"
		tr, err := httpfetch.NewTransport(cfg)
		if err != nil {
			t.Fatalf("NewTransport error: %v", err)
		}
		if tr.Proxy == nil {
			t.Errorf("expected tr.Proxy to be configured for manual HTTP proxy")
		}
	})

	t.Run("Manual_SOCKS5", func(t *testing.T) {
		cfg := httpfetch.DefaultTransportConfig()
		cfg.ProxyMode = httpfetch.ProxyModeManual
		cfg.ProxyURL = "socks5://127.0.0.1:1080"
		tr, err := httpfetch.NewTransport(cfg)
		if err != nil {
			t.Fatalf("NewTransport error: %v", err)
		}
		if tr.DialContext == nil {
			t.Errorf("expected DialContext to be set for SOCKS5 proxy")
		}
	})

	t.Run("Manual_Invalid", func(t *testing.T) {
		cfg := httpfetch.DefaultTransportConfig()
		cfg.ProxyMode = httpfetch.ProxyModeManual
		cfg.ProxyURL = "ftp://invalid-proxy"
		_, err := httpfetch.NewTransport(cfg)
		if err == nil {
			t.Errorf("expected error for unsupported ftp proxy scheme")
		}
	})
}
