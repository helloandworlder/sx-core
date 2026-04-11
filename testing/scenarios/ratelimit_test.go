package scenarios

import (
	"bytes"
	"context"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/ratelimit"
	"github.com/xtls/xray-core/common"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/distro/all"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/proxy/socks"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
)

func TestSocksRateLimitLongDownload(t *testing.T) {
	const (
		username       = "rate-user"
		password       = "rate-pass"
		email          = "rate-user@example.com"
		rateBps  int64 = 125000
		bodySize       = 3 * 1024 * 1024
		minDuration    = 20 * time.Second
		maxDuration    = 45 * time.Second
	)

	payload := bytes.Repeat([]byte("r"), bodySize)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "payload.bin", time.Unix(0, 0), bytes.NewReader(payload))
	}))
	defer httpServer.Close()

	socksPort := tcp.PickPort()
	cfg := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{
				Level: map[uint32]*policy.Policy{
					0: {
						Timeout: &policy.Policy_Timeout{
							ConnectionIdle: &policy.Second{Value: 600},
							UplinkOnly:     &policy.Second{Value: 300},
							DownlinkOnly:   &policy.Second{Value: 300},
						},
					},
				},
			}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &xnet.PortList{Range: []*xnet.PortRange{xnet.SinglePortRange(socksPort)}},
					Listen:   xnet.NewIPOrDomain(xnet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&socks.ServerConfig{
					AuthType: socks.AuthType_PASSWORD,
					Accounts: map[string]string{
						username: password,
					},
					AccountEmails: map[string]string{
						username: email,
					},
					Address:    xnet.NewIPOrDomain(xnet.LocalHostIP),
					UdpEnabled: false,
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
			},
		},
	}

	cfgBytes, err := proto.Marshal(cfg)
	common.Must(err)

	ratelimit.Manager.Set(email, rateBps, rateBps)
	defer ratelimit.Manager.Remove(email)

	instance, err := core.StartInstance("protobuf", cfgBytes)
	common.Must(err)
	defer instance.Close()

	dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), &proxy.Auth{
		User:     username,
		Password: password,
	}, proxy.Direct)
	common.Must(err)

	client := &http.Client{
		Timeout: 90 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, address string) (stdnet.Conn, error) {
				return dialer.Dial(network, address)
			},
		},
	}

	fetch := func(expectBytes int, minExpected time.Duration) {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, httpServer.URL, nil)
		common.Must(err)
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", expectBytes-1))

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("proxy request failed: %v", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		duration := time.Since(start)
		if err != nil {
			t.Fatalf("proxy body read failed after %v with %d/%d bytes: %v", duration, len(body), expectBytes, err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
		if len(body) != expectBytes {
			t.Fatalf("short body: got %d want %d", len(body), expectBytes)
		}
		if duration < minExpected {
			t.Fatalf("download finished too fast: %v < %v", duration, minExpected)
		}
		if duration > maxDuration {
			t.Fatalf("download finished too slow: %v > %v", duration, maxDuration)
		}
	}

	fetch(bodySize, minDuration)
	fetch(256*1024, 2*time.Second)
}
