package httpfetch_test

import (
	"arus/internal/adapter/httpfetch"
	"arus/internal/domain"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestSessionContext_CookieScoping_NoLeakOnCrossHost(t *testing.T) {
	var crossHostReceivedCookie atomic.Bool
	var primaryReceivedCookie atomic.Bool

	crossHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" || len(r.Cookies()) > 0 {
			crossHostReceivedCookie.Store(true)
		}
		w.Header().Set("Content-Range", "bytes 0-99/100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer crossHost.Close()

	crossHostURL, err := url.Parse(crossHost.URL)
	if err != nil {
		t.Fatalf("url.Parse error: %v", err)
	}
	crossHostURL.Host = "localhost:" + crossHostURL.Port()

	primaryHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" || len(r.Cookies()) > 0 {
			primaryReceivedCookie.Store(true)
		}
		http.Redirect(w, r, crossHostURL.String()+"/file.bin", http.StatusFound)
	}))
	defer primaryHost.Close()

	primaryURL, err := url.Parse(primaryHost.URL)
	if err != nil {
		t.Fatalf("url.Parse error: %v", err)
	}

	rc := domain.RequestContext{
		RawCookie:       "secret_token=12345; auth_session=abcdef",
		CookieScopeHost: primaryURL.Hostname(),
		Cookies: []domain.Cookie{
			{
				Name:   "structured_cookie",
				Value:  "foo",
				Domain: primaryURL.Hostname(),
				Path:   "/",
			},
		},
	}

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{
		MaxRedirects: 5,
	})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	resp, err := fetcher.OpenRange(t.Context(), rc, primaryHost.URL+"/start", 0, 99, "")
	if err != nil {
		t.Fatalf("OpenRange error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if !primaryReceivedCookie.Load() {
		t.Errorf("primary host should have received cookies")
	}
	if crossHostReceivedCookie.Load() {
		t.Errorf("cross host MUST NOT receive scoped cookies")
	}
}

func TestSessionContext_RefererPolicy(t *testing.T) {
	fromHTTPS, _ := url.Parse("https://origin.com/sub/page.html")
	toHTTPSCross, _ := url.Parse("https://cdn.other.com/file.bin")
	toHTTPCross, _ := url.Parse("http://insecure.other.com/file.bin")
	toHTTPSSame, _ := url.Parse("https://origin.com/file.bin")

	originalRef := "https://origin.com/sub/page.html"

	sameResult := httpfetch.FilterRefererOnRedirect(originalRef, fromHTTPS, toHTTPSSame)
	if sameResult != originalRef {
		t.Errorf("same-origin referer = %q, want %q", sameResult, originalRef)
	}

	crossResult := httpfetch.FilterRefererOnRedirect(originalRef, fromHTTPS, toHTTPSCross)
	expectedCross := "https://origin.com/"
	if crossResult != expectedCross {
		t.Errorf("cross-origin referer = %q, want %q", crossResult, expectedCross)
	}

	downgradeResult := httpfetch.FilterRefererOnRedirect(originalRef, fromHTTPS, toHTTPCross)
	if downgradeResult != "" {
		t.Errorf("downgrade referer = %q, want empty string", downgradeResult)
	}
}

func TestSessionContext_UpdateJarFromResponse(t *testing.T) {
	u, _ := url.Parse("https://example.com/login")
	jar, err := httpfetch.BuildCookieJar(domain.RequestContext{}, u)
	if err != nil {
		t.Fatalf("BuildCookieJar error: %v", err)
	}

	header := http.Header{}
	header.Add("Set-Cookie", "session_id=new_session_999; Path=/; Domain=example.com; Secure")

	httpfetch.UpdateJarFromResponse(jar, u, header)

	reqURL, _ := url.Parse("https://example.com/dashboard")
	cookies := jar.Cookies(reqURL)
	found := false
	for _, c := range cookies {
		if c.Name == "session_id" && c.Value == "new_session_999" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("cookie set by response was not stored in jar")
	}
}
