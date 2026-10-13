package domain_test

import (
	"arus/internal/domain"
	"net/http"
	"testing"
)

func TestMatchRefresh_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		job     domain.Download
		capture domain.Capture
		wantOK  bool
	}{
		{
			name: "match tepat",
			job: domain.Download{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip",
				Validators: domain.Validators{
					ETag: `"etag-12345"`,
				},
			},
			capture: domain.Capture{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip?token=new",
				Headers: http.Header{
					"ETag": []string{`"etag-12345"`},
				},
			},
			wantOK: true,
		},
		{
			name: "size beda",
			job: domain.Download{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip",
				Validators: domain.Validators{
					ETag: `"etag-12345"`,
				},
			},
			capture: domain.Capture{
				FileName:  "archive.zip",
				TotalSize: 209715200,
				URL:       "https://cdn.example.com/downloads/archive.zip?token=new",
				Headers: http.Header{
					"ETag": []string{`"etag-12345"`},
				},
			},
			wantOK: false,
		},
		{
			name: "ETag beda",
			job: domain.Download{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip",
				Validators: domain.Validators{
					ETag: `"etag-12345"`,
				},
			},
			capture: domain.Capture{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip?token=new",
				Headers: http.Header{
					"ETag": []string{`"etag-99999"`},
				},
			},
			wantOK: false,
		},
		{
			name: "nama beda tapi size+ETag sama",
			job: domain.Download{
				FileName:  "archive.zip",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/downloads/archive.zip",
				Validators: domain.Validators{
					ETag: `W/"etag-12345"`,
				},
			},
			capture: domain.Capture{
				FileName:  "generated_download_token_99.bin",
				TotalSize: 104857600,
				URL:       "https://cdn.example.com/stream?token=abc",
				Headers: http.Header{
					"ETag": []string{`"etag-12345"`},
				},
			},
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			score, ok := domain.MatchRefresh(tc.job, tc.capture)
			if ok != tc.wantOK {
				t.Fatalf("MatchRefresh() ok = %v (score=%d), wantOK = %v", ok, score, tc.wantOK)
			}
			if tc.wantOK && score < 50 {
				t.Fatalf("MatchRefresh() score = %d, want >= 50", score)
			}
			if !tc.wantOK && score > 0 {
				t.Fatalf("MatchRefresh() expected score 0 on hard mismatch, got %d", score)
			}
		})
	}
}
