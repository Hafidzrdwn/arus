package observability_test

import (
	"arus/internal/observability"
	"net/http"
	"testing"
)

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "url without query is unchanged",
			input:    "https://example.com/download/archive.tar.gz",
			expected: "https://example.com/download/archive.tar.gz",
		},
		{
			name:     "url with hash but no query is unchanged",
			input:    "https://example.com/index.html#section2",
			expected: "https://example.com/index.html#section2",
		},
		{
			name:     "url with token param",
			input:    "https://example.com/get?token=secret12345",
			expected: "https://example.com/get?token=[REDACTED]",
		},
		{
			name:     "url with X-Amz-Signature",
			input:    "https://s3.amazonaws.com/bucket/file.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abcdef123456",
			expected: "https://s3.amazonaws.com/bucket/file.zip?X-Amz-Algorithm=[REDACTED]&X-Amz-Signature=[REDACTED]",
		},
		{
			name:     "url with x-amz-* lowercase prefix",
			input:    "https://s3.amazonaws.com/bucket/file.iso?x-amz-security-token=AQoDYXdzEJr1&file_id=42",
			expected: "https://s3.amazonaws.com/bucket/file.iso?x-amz-security-token=[REDACTED]&file_id=42",
		},
		{
			name:     "url with sig and signature params",
			input:    "https://example.com/file?sig=998877&signature=aabbcc",
			expected: "https://example.com/file?sig=[REDACTED]&signature=[REDACTED]",
		},
		{
			name:     "url with key and auth params",
			input:    "https://api.example.com/data?key=api-key-999&auth=basic-token&format=json",
			expected: "https://api.example.com/data?key=[REDACTED]&auth=[REDACTED]&format=json",
		},
		{
			name:     "url with case variations (KEY, TOKEN, Sig)",
			input:    "https://example.com/download?KEY=secret&TOKEN=pass&Sig=hash",
			expected: "https://example.com/download?KEY=[REDACTED]&TOKEN=[REDACTED]&Sig=[REDACTED]",
		},
		{
			name:     "url with non-sensitive params only",
			input:    "https://example.com/search?q=golang&page=2&sort=desc",
			expected: "https://example.com/search?q=golang&page=2&sort=desc",
		},
		{
			name:     "url with mixed sensitive and non-sensitive params",
			input:    "https://example.com/file.bin?id=101&token=xyz789&verbose=true",
			expected: "https://example.com/file.bin?id=101&token=[REDACTED]&verbose=true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := observability.RedactURL(tc.input)
			if actual != tc.expected {
				t.Errorf("RedactURL(%q)\n got:  %q\n want: %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestRedactHeaders(t *testing.T) {
	tests := []struct {
		name     string
		input    http.Header
		validate func(t *testing.T, redacted http.Header)
	}{
		{
			name:  "nil header",
			input: nil,
			validate: func(t *testing.T, redacted http.Header) {
				if redacted != nil {
					t.Errorf("expected nil header, got: %#v", redacted)
				}
			},
		},
		{
			name: "cookie, authorization, and set-cookie redacted",
			input: http.Header{
				"Cookie":          []string{"session_id=12345; user=alice"},
				"Authorization":   []string{"Bearer super-secret-token"},
				"Set-Cookie":      []string{"id=a3fWa; Expires=Wed, 21 Oct 2026 07:28:00 GMT; Secure; HttpOnly"},
				"User-Agent":      []string{"Arus/1.0"},
				"Accept-Encoding": []string{"identity"},
			},
			validate: func(t *testing.T, redacted http.Header) {
				if got := redacted.Get("Cookie"); got != "[REDACTED]" {
					t.Errorf("expected Cookie [REDACTED], got: %q", got)
				}
				if got := redacted.Get("Authorization"); got != "[REDACTED]" {
					t.Errorf("expected Authorization [REDACTED], got: %q", got)
				}
				if got := redacted.Get("Set-Cookie"); got != "[REDACTED]" {
					t.Errorf("expected Set-Cookie [REDACTED], got: %q", got)
				}
				if got := redacted.Get("User-Agent"); got != "Arus/1.0" {
					t.Errorf("expected User-Agent Arus/1.0, got: %q", got)
				}
				if got := redacted.Get("Accept-Encoding"); got != "identity" {
					t.Errorf("expected Accept-Encoding identity, got: %q", got)
				}
			},
		},
		{
			name: "case insensitive header redaction",
			input: http.Header{
				"cookie":        []string{"secret_cookie"},
				"AUTHORIZATION": []string{"Basic dXNlcjpwYXNz"},
				"set-cookie":    []string{"token=123"},
			},
			validate: func(t *testing.T, redacted http.Header) {
				for _, k := range []string{"cookie", "AUTHORIZATION", "set-cookie"} {
					vals := redacted[k]
					if len(vals) != 1 || vals[0] != "[REDACTED]" {
						t.Errorf("expected header %q to be [REDACTED], got: %v", k, vals)
					}
				}
			},
		},
		{
			name: "does not mutate original header",
			input: http.Header{
				"Cookie": []string{"secret"},
			},
			validate: func(t *testing.T, redacted http.Header) {
				// original check handled outside
				if redacted.Get("Cookie") != "[REDACTED]" {
					t.Errorf("expected redacted header to have [REDACTED]")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var origCopy http.Header
			if tc.input != nil {
				origCopy = tc.input.Clone()
			}
			redacted := observability.RedactHeaders(tc.input)
			tc.validate(t, redacted)

			if tc.input != nil {
				// verify original was not modified
				for k, v := range origCopy {
					origVals := tc.input[k]
					if len(origVals) != len(v) {
						t.Errorf("original header %s mutated: got %v, want %v", k, origVals, v)
					}
					for i := range v {
						if origVals[i] != v[i] {
							t.Errorf("original header %s[%d] mutated: got %q, want %q", k, i, origVals[i], v[i])
						}
					}
				}
			}
		})
	}
}
