package ratelimit

import (
	"context"
	"io"
)

// RateLimitedReader wraps an io.Reader with a token bucket rate limiter.
// It reads from the underlying reader and then waits for tokens before returning.
type RateLimitedReader struct {
	reader  io.Reader
	bucket  *TokenBucket
	tracker func(int64) // optional: track bytes for speed measurement
}

// NewRateLimitedReader creates a rate-limited reader.
func NewRateLimitedReader(r io.Reader, bucket *TokenBucket, tracker func(int64)) *RateLimitedReader {
	return &RateLimitedReader{
		reader:  r,
		bucket:  bucket,
		tracker: tracker,
	}
}

func (r *RateLimitedReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		if r.bucket != nil {
			r.bucket.Wait(int64(n))
		}
		if r.tracker != nil {
			r.tracker(int64(n))
		}
	}
	return
}

// RateLimitedWriter wraps an io.Writer with a token bucket rate limiter.
type RateLimitedWriter struct {
	writer  io.Writer
	bucket  *TokenBucket
	tracker func(int64)
}

// NewRateLimitedWriter creates a rate-limited writer.
func NewRateLimitedWriter(w io.Writer, bucket *TokenBucket, tracker func(int64)) *RateLimitedWriter {
	return &RateLimitedWriter{
		writer:  w,
		bucket:  bucket,
		tracker: tracker,
	}
}

func (w *RateLimitedWriter) Write(p []byte) (n int, err error) {
	if w.bucket != nil {
		w.bucket.Wait(int64(len(p)))
	}
	n, err = w.writer.Write(p)
	if n > 0 && w.tracker != nil {
		w.tracker(int64(n))
	}
	return
}

// WrapLink wraps ingress and egress directions of a data transfer with rate limiting.
// In XrayCore context:
//   - egressReader: data flowing from user to proxy (user upload = proxy egress)
//   - ingressWriter: data flowing from proxy to user (user download = proxy ingress)
//
// The email parameter looks up the per-user limiter from the global Manager.
// If no limiter is set for this email, returns the original reader/writer unchanged.
func WrapLink(ctx context.Context, email string, upReader io.Reader, downWriter io.Writer) (io.Reader, io.Writer) {
	if email == "" {
		return upReader, downWriter
	}

	ul := Manager.Get(email)
	if ul == nil {
		return upReader, downWriter
	}

	wrappedReader := upReader
	if ul.Egress != nil {
		wrappedReader = NewRateLimitedReader(upReader, ul.Egress, ul.TrackEgress)
	}

	wrappedWriter := downWriter
	if ul.Ingress != nil {
		wrappedWriter = NewRateLimitedWriter(downWriter, ul.Ingress, ul.TrackIngress)
	}

	return wrappedReader, wrappedWriter
}
