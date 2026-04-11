package dispatcher

import (
	"github.com/xtls/xray-core/app/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/signal"
)

// rateLimitStatWriter wraps a buf.Writer with a token bucket rate limiter.
type rateLimitStatWriter struct {
	Writer  buf.Writer
	bucket  *ratelimit.TokenBucket
	tracker func(int64)
	activity signal.ActivityUpdater
}

const rateLimitChunkSize = 16 * 1024

func (w *rateLimitStatWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for !mb.IsEmpty() {
		var chunk buf.MultiBuffer
		mb, chunk = buf.SplitSize(mb, rateLimitChunkSize)
		nBytes := int64(chunk.Len())
		if nBytes > 0 {
			if w.bucket != nil {
				w.bucket.Wait(nBytes)
			}
			if w.tracker != nil {
				w.tracker(nBytes)
			}
		}
		if err := w.Writer.WriteMultiBuffer(chunk); err != nil {
			return err
		}
		if w.activity != nil {
			w.activity.Update()
		}
	}
	return nil
}

func (w *rateLimitStatWriter) SetActivity(activity signal.ActivityUpdater) {
	w.activity = activity
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
	buffer  buf.MultiBuffer
	nextErr error
	activity signal.ActivityUpdater
}

func (r *rateLimitStatReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.buffer.IsEmpty() {
		mb, err := r.Reader.ReadMultiBuffer()
		if mb == nil {
			return nil, err
		}
		r.buffer = mb
		r.nextErr = err
	}

	var chunk buf.MultiBuffer
	r.buffer, chunk = buf.SplitSize(r.buffer, rateLimitChunkSize)
	nBytes := int64(chunk.Len())
	if nBytes > 0 {
		if r.bucket != nil {
			r.bucket.Wait(nBytes)
		}
		if r.tracker != nil {
			r.tracker(nBytes)
		}
	}

	if r.buffer.IsEmpty() {
		err := r.nextErr
		r.nextErr = nil
		if r.activity != nil {
			r.activity.Update()
		}
		return chunk, err
	}

	if r.activity != nil {
		r.activity.Update()
	}
	return chunk, nil
}

func (r *rateLimitStatReader) SetActivity(activity signal.ActivityUpdater) {
	r.activity = activity
}

func (r *rateLimitStatReader) Interrupt() {
	common.Interrupt(r.Reader)
}
