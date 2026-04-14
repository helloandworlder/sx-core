package reverse

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
)

const (
	internalDomain = "reverse"
)

func isDomain(dest net.Destination, domain string) bool {
	return dest.Address.Family().IsDomain() && dest.Address.Domain() == domain
}

func isInternalDomain(dest net.Destination) bool {
	return isDomain(dest, internalDomain)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		r := new(Reverse)
		if err := core.RequireFeatures(ctx, func(d routing.Dispatcher, om outbound.Manager) error {
			return r.Init(config.(*Config), d, om)
		}); err != nil {
			return nil, err
		}
		return r, nil
	}))
}

type Reverse struct {
	bridges []*Bridge
	portals []*Portal
	dispatcher routing.Dispatcher
	ohm        outbound.Manager
	running    bool
	mu         sync.Mutex
}

func (r *Reverse) Init(config *Config, d routing.Dispatcher, ohm outbound.Manager) error {
	r.dispatcher = d
	r.ohm = ohm
	return r.replaceConfigLocked(config)
}

func (r *Reverse) replaceConfigLocked(config *Config) error {
	bridges := make([]*Bridge, 0, len(config.BridgeConfig))
	portals := make([]*Portal, 0, len(config.PortalConfig))

	for _, bConfig := range config.BridgeConfig {
		b, err := NewBridge(bConfig, r.dispatcher)
		if err != nil {
			return err
		}
		bridges = append(bridges, b)
	}

	for _, pConfig := range config.PortalConfig {
		p, err := NewPortal(pConfig, r.ohm)
		if err != nil {
			for _, bridge := range bridges {
				_ = bridge.Close()
			}
			return err
		}
		portals = append(portals, p)
	}

	oldBridges := r.bridges
	oldPortals := r.portals

	if r.running {
		for _, bridge := range oldBridges {
			_ = bridge.Close()
		}
		for _, portal := range oldPortals {
			_ = portal.Close()
		}
		for _, bridge := range bridges {
			if err := bridge.Start(); err != nil {
				return err
			}
		}
		for _, portal := range portals {
			if err := portal.Start(); err != nil {
				return err
			}
		}
	}

	r.bridges = bridges
	r.portals = portals

	return nil
}

func (r *Reverse) Type() interface{} {
	return (*Reverse)(nil)
}

func (r *Reverse) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.bridges {
		if err := b.Start(); err != nil {
			return err
		}
	}

	for _, p := range r.portals {
		if err := p.Start(); err != nil {
			return err
		}
	}
	r.running = true

	return nil
}

func (r *Reverse) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, b := range r.bridges {
		errs = append(errs, b.Close())
	}

	for _, p := range r.portals {
		errs = append(errs, p.Close())
	}
	r.running = false

	return errors.Combine(errs...)
}

func (r *Reverse) ReplaceConfig(config *Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.replaceConfigLocked(config)
}
