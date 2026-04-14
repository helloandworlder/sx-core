package command

import (
	"context"

	"github.com/xtls/xray-core/app/commander"
	appreverse "github.com/xtls/xray-core/app/reverse"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"google.golang.org/grpc"
)

type reverseServer struct {
	reverse *appreverse.Reverse
}

func (s *reverseServer) ReplaceConfig(ctx context.Context, request *ReplaceConfigRequest) (*ReplaceConfigResponse, error) {
	if s.reverse == nil {
		return nil, errors.New("reverse feature not available")
	}
	if request == nil || request.Config == nil {
		return nil, errors.New("reverse config is required")
	}
	return &ReplaceConfigResponse{}, s.reverse.ReplaceConfig(request.Config)
}

func (s *reverseServer) mustEmbedUnimplementedReverseServiceServer() {}

type service struct {
	v *core.Instance
}

func (s *service) Register(server *grpc.Server) {
	rs := &reverseServer{}
	common.Must(s.v.RequireFeatures(func(reverse *appreverse.Reverse) {
		rs.reverse = reverse
	}, false))
	RegisterReverseServiceServer(server, rs)

	vCoreDesc := ReverseService_ServiceDesc
	vCoreDesc.ServiceName = "v2ray.core.app.reverse.command.ReverseService"
	server.RegisterService(&vCoreDesc, rs)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		s := core.MustFromContext(ctx)
		return &service{v: s}, nil
	}))
}

var _ commander.Service = (*service)(nil)
