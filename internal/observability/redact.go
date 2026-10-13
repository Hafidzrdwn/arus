package observability

import (
	"net/http"
	"net/url"
	"strings"
)

// RedactionPlaceholder is the masking replacement used for sensitive values.
const RedactionPlaceholder = "[REDACTED]"

func isSensitiveQueryKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "token", "sig", "signature", "key", "auth", "secret", "password", "pwd", "credential":
		return true
	}
	if strings.HasPrefix(lower, "x-amz-") {
		return true
	}
	if strings.HasSuffix(lower, "token") || strings.HasSuffix(lower, "_token") || strings.HasSuffix(lower, "-token") {
		return true
	}
	if strings.HasSuffix(lower, "key") || strings.HasSuffix(lower, "_key") || strings.HasSuffix(lower, "-key") {
		return true
	}
	if strings.HasSuffix(lower, "secret") || strings.HasSuffix(lower, "_secret") {
		return true
	}
	if strings.HasSuffix(lower, "signature") || strings.HasSuffix(lower, "_sig") || strings.HasSuffix(lower, "-sig") {
		return true
	}
	if strings.HasPrefix(lower, "api_") || strings.HasPrefix(lower, "auth_") {
		return true
	}
	return false
}

// RedactURL masks sensitive query parameter values with "[REDACTED]".
func RedactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}

	u, err := url.Parse(rawURL)
	if err != nil || u.RawQuery == "" {
		return rawURL
	}

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

func isSensitiveHeader(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "cookie", "set-cookie", "authorization", "proxy-authorization", "x-api-key", "x-auth-token":
		return true
	}
	if strings.Contains(lower, "authorization") || strings.HasPrefix(lower, "x-auth") || strings.HasPrefix(lower, "x-api-key") || strings.HasSuffix(lower, "-token") {
		return true
	}
	return false
}

// RedactHeaders returns a copy of h with sensitive authentication headers masked with "[REDACTED]".
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
