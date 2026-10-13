package httpfetch

import (
	"arus/internal/domain"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var allowedHeaderNames = map[string]bool{
	"user-agent":      true,
	"accept":          true,
	"accept-language": true,
	"referer":         true,
	"origin":          true,
	"authorization":   true,
}

var deniedHeaderNames = map[string]bool{
	"host":                true,
	"content-length":      true,
	"range":               true,
	"connection":          true,
	"accept-encoding":     true,
	"transfer-encoding":   true,
	"keep-alive":          true,
	"proxy-connection":    true,
	"trailer":             true,
	"upgrade":             true,
	"te":                  true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
}

// IsHeaderAllowed determines whether a header from RequestContext may be transmitted.
func IsHeaderAllowed(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if deniedHeaderNames[lower] || strings.HasPrefix(lower, "if-") {
		return false
	}
	if allowedHeaderNames[lower] || strings.HasPrefix(lower, "x-") {
		return true
	}
	return false
}

// ApplyRequestContext applies allowlisted headers, user agent, and referer to req.
func ApplyRequestContext(req *http.Request, rc domain.RequestContext) {
	for k, vals := range rc.Headers {
		if IsHeaderAllowed(k) {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
	}

	if rc.UserAgent != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", rc.UserAgent)
	}

	if rc.Referer != "" && req.Header.Get("Referer") == "" {
		req.Header.Set("Referer", rc.Referer)
	}
}

// BuildCookieJar initializes an http.CookieJar populated with structured and raw cookies
// scoped by registrable domain using publicsuffix.List.
func BuildCookieJar(rc domain.RequestContext, targetURL *url.URL) (*cookiejar.Jar, error) {
	jar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		return nil, err
	}

	for _, c := range rc.Cookies {
		domainStr := strings.TrimPrefix(c.Domain, ".")
		if domainStr == "" && targetURL != nil {
			domainStr = targetURL.Hostname()
		}

		u := &url.URL{
			Scheme: "https",
			Host:   domainStr,
			Path:   c.Path,
		}
		if u.Path == "" {
			u.Path = "/"
		}

		hc := &http.Cookie{
			Name:     c.Name,
			Value:    c.Value,
			Path:     u.Path,
			Domain:   domainStr,
			Expires:  c.Expires,
			Secure:   c.Secure,
			HttpOnly: false,
		}
		jar.SetCookies(u, []*http.Cookie{hc})
	}

	if rc.RawCookie != "" {
		host := rc.CookieScopeHost
		if host == "" && targetURL != nil {
			host = targetURL.Hostname()
		}
		host = strings.TrimPrefix(host, ".")

		if host != "" {
			u := &url.URL{
				Scheme: "https",
				Host:   host,
				Path:   "/",
			}
			parts := strings.Split(rc.RawCookie, ";")
			var rawCookies []*http.Cookie
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				eq := strings.IndexByte(part, '=')
				if eq > 0 {
					name := strings.TrimSpace(part[:eq])
					val := strings.TrimSpace(part[eq+1:])
					rawCookies = append(rawCookies, &http.Cookie{
						Name:   name,
						Value:  val,
						Path:   "/",
						Domain: host,
					})
				}
			}
			if len(rawCookies) > 0 {
				jar.SetCookies(u, rawCookies)
			}
		}
	}

	return jar, nil
}

// FilterRefererOnRedirect computes the updated Referer header for a redirect hop
// applying the strict-origin-when-cross-origin policy.
func FilterRefererOnRedirect(originalReferer string, fromURL, toURL *url.URL) string {
	if originalReferer == "" {
		return ""
	}

	if fromURL.Scheme == "https" && toURL.Scheme == "http" {
		return ""
	}

	if fromURL.Scheme == toURL.Scheme && fromURL.Host == toURL.Host {
		return originalReferer
	}

	return fromURL.Scheme + "://" + fromURL.Host + "/"
}

// StripHeadersOnCrossHost removes sensitive headers when redirecting across distinct host origins.
func StripHeadersOnCrossHost(req *http.Request, fromURL, toURL *url.URL) {
	if fromURL.Hostname() != toURL.Hostname() {
		req.Header.Del("Authorization")
	}

	if ref := req.Header.Get("Referer"); ref != "" {
		filtered := FilterRefererOnRedirect(ref, fromURL, toURL)
		if filtered == "" {
			req.Header.Del("Referer")
		} else {
			req.Header.Set("Referer", filtered)
		}
	}
}
