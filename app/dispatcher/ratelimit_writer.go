package dispatcher

import (
	"github.com/xtls/xray-core/app/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// rateLimitStatWriter wraps a buf.Writer with a token bucket rate limiter.
type rateLimitStatWriter struct {
	Writer  buf.Writer
	bucket  *ratelimit.TokenBucket
	tracker func(int64)
}

func (w *rateLimitStatWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	nBytes := int64(mb.Len())
	if nBytes > 0 {
		w.bucket.Wait(nBytes)
		if w.tracker != nil {
			w.tracker(nBytes)
		}
	}
	return w.Writer.WriteMultiBuffer(mb)
}

func (w *rateLimitStatWriter) Close() error {
	return common.Close(w.Writer)
}

func (w *rateLimitStatWriter) Interrupt() {
	common.Interrupt(w.Writer)
}

// rateLimitStatReader wraps a buf.Reader with a token bucket rate limiter.
type rateLimitStatReader struct {
	Reader  buf.Reader
	bucket  *ratelimit.TokenBucket
	tracker func(int64)
}

func (r *rateLimitStatReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.Reader.ReadMultiBuffer()
	if mb != nil {
		nBytes := int64(mb.Len())
		if nBytes > 0 && r.bucket != nil {
			r.bucket.Wait(nBytes)
		}
		if nBytes > 0 && r.tracker != nil {
			r.tracker(nBytes)
		}
	}
	return mb, err
}

func (r *rateLimitStatReader) Interrupt() {
	common.Interrupt(r.Reader)
}
