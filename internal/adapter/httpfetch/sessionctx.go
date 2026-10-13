package httpfetch

import (
	"arus/internal/domain"
	"net"
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
	if deniedHeaderNames[lower] || strings.HasPrefix(lower, "if-") || strings.HasPrefix(lower, "sec-fetch-") {
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

// BuildCookieJar initializes an http.CookieJar populated with structured and raw cookies.
func BuildCookieJar(rc domain.RequestContext, targetURL *url.URL) (*cookiejar.Jar, error) {
	jar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		return nil, err
	}

	defaultScheme := "https"
	if targetURL != nil && targetURL.Scheme != "" {
		defaultScheme = targetURL.Scheme
	}

	for _, c := range rc.Cookies {
		domainStr := strings.TrimPrefix(c.Domain, ".")
		if domainStr == "" && targetURL != nil {
			domainStr = targetURL.Hostname()
		}

		scheme := defaultScheme
		if c.Secure {
			scheme = "https"
		}

		cookieDomain := domainStr
		if net.ParseIP(domainStr) != nil || domainStr == "localhost" {
			cookieDomain = ""
		}

		u := &url.URL{
			Scheme: scheme,
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
			Domain:   cookieDomain,
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
		host = strings.TrimPrefix(strings.TrimSpace(host), ".")

		if host != "" {
			scheme := defaultScheme
			regDomain := host
			if eTLD, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil && eTLD != "" {
				regDomain = eTLD
			}
			cookieDomain := regDomain
			if net.ParseIP(regDomain) != nil || regDomain == "localhost" {
				cookieDomain = ""
			}

			u := &url.URL{
				Scheme: scheme,
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
						Domain: cookieDomain,
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

// UpdateJarFromResponse updates the cookie jar with Set-Cookie headers from an HTTP response.
func UpdateJarFromResponse(jar http.CookieJar, u *url.URL, header http.Header) {
	if jar == nil || u == nil {
		return
	}
	resp := http.Response{Header: header}
	cookies := resp.Cookies()
	if len(cookies) > 0 {
		jar.SetCookies(u, cookies)
	}
}

// FilterRefererOnRedirect computes the updated Referer header for a redirect hop.
func FilterRefererOnRedirect(originalReferer string, fromURL, toURL *url.URL) string {
	if originalReferer == "" {
		return ""
	}

	refURL, err := url.Parse(originalReferer)
	if err != nil {
		return ""
	}

	if (refURL.Scheme == "https" || (fromURL != nil && fromURL.Scheme == "https")) && toURL.Scheme == "http" {
		return ""
	}

	if refURL.Scheme == toURL.Scheme && refURL.Host == toURL.Host {
		return originalReferer
	}

	return refURL.Scheme + "://" + refURL.Host + "/"
}

// StripHeadersOnCrossHost removes sensitive headers when redirecting across distinct host origins.
func StripHeadersOnCrossHost(req *http.Request, fromURL, toURL *url.URL) {
	if fromURL.Hostname() != toURL.Hostname() {
		req.Header.Del("Authorization")
		req.Header.Del("Cookie")
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
