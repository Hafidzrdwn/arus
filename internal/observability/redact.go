package observability

import (
	"net/http"
	"net/url"
	"strings"
)

// RedactionPlaceholder is the masking replacement used for sensitive values.
const RedactionPlaceholder = "[REDACTED]"

// isSensitiveQueryKey checks if a query parameter key matches sensitive patterns:
// token, sig, signature, key, auth, or x-amz-* (case-insensitive).
func isSensitiveQueryKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "token", "sig", "signature", "key", "auth":
		return true
	}
	return strings.HasPrefix(lower, "x-amz-")
}

// RedactURL masks sensitive query parameter values (token, sig, signature, key,
// auth, x-amz-*) with "[REDACTED]". URLs without a query string are returned unmodified.
func RedactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}

	u, err := url.Parse(rawURL)
	if err != nil || u.RawQuery == "" {
		return rawURL
	}

	// Split by '&' to preserve the original order of query parameters.
	params := strings.Split(u.RawQuery, "&")
	modified := false
	for i, param := range params {
		if param == "" {
			continue
		}
		key, _, _ := strings.Cut(param, "=")
		unescapedKey, err := url.QueryUnescape(key)
		if err != nil {
			unescapedKey = key
		}

		if isSensitiveQueryKey(unescapedKey) {
			params[i] = key + "=" + RedactionPlaceholder
			modified = true
		}
	}

	if !modified {
		return rawURL
	}

	u.RawQuery = strings.Join(params, "&")
	return u.String()
}

// isSensitiveHeader returns true for sensitive HTTP headers: Cookie, Authorization, Set-Cookie.
func isSensitiveHeader(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "cookie", "authorization", "set-cookie":
		return true
	default:
		return false
	}
}

// RedactHeaders returns a deep clone of the given http.Header with sensitive headers
// (Cookie, Authorization, Set-Cookie) masked with "[REDACTED]".
func RedactHeaders(h http.Header) http.Header {
	if h == nil {
		return nil
	}

	redacted := make(http.Header, len(h))
	for k, vals := range h {
		if isSensitiveHeader(k) {
			redacted[k] = []string{RedactionPlaceholder}
		} else {
			cp := make([]string, len(vals))
			copy(cp, vals)
			redacted[k] = cp
		}
	}
	return redacted
}
