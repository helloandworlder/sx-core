package command

import (
	"context"

	"github.com/xtls/xray-core/app/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rateLimitServer struct{}

func NewRateLimitServer() RateLimitServiceServer {
	return &rateLimitServer{}
}

func toProtoInfo(info ratelimit.UserSpeedInfo) *UserRateLimit {
	return &UserRateLimit{
		Email:                info.Email,
		EgressLimitBps:       info.EgressLimitBps,
		IngressLimitBps:      info.IngressLimitBps,
		EgressBps:            info.EgressBps,
		IngressBps:           info.IngressBps,
		BurstEgressLimitBps:  info.BurstEgressLimitBps,
		BurstIngressLimitBps: info.BurstIngressLimitBps,
		BurstDurationSeconds: info.BurstDurationSeconds,
		BurstCooldownSeconds: info.BurstCooldownSeconds,
	}
}

func (s *rateLimitServer) SetUserRateLimit(ctx context.Context, request *SetUserRateLimitRequest) (*SetUserRateLimitResponse, error) {
	if request.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	ratelimit.Manager.SetWithBurst(
		request.GetEmail(),
		request.GetEgressBps(),
		request.GetIngressBps(),
		request.GetBurstEgressBps(),
		request.GetBurstIngressBps(),
		request.GetBurstDurationSeconds(),
		request.GetBurstCooldownSeconds(),
	)
	return &SetUserRateLimitResponse{}, nil
}

func (s *rateLimitServer) GetUserRateLimit(ctx context.Context, request *GetUserRateLimitRequest) (*GetUserRateLimitResponse, error) {
	info := ratelimit.GetUserRateLimit(request.GetEmail())
	if info == nil {
		return nil, status.Error(codes.NotFound, request.GetEmail()+" not found.")
	}
	return &GetUserRateLimitResponse{Info: toProtoInfo(*info)}, nil
}

func (s *rateLimitServer) RemoveUserRateLimit(ctx context.Context, request *RemoveUserRateLimitRequest) (*RemoveUserRateLimitResponse, error) {
	if request.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	ratelimit.Manager.Remove(request.GetEmail())
	return &RemoveUserRateLimitResponse{}, nil
}

func (s *rateLimitServer) GetUserSpeed(ctx context.Context, request *GetUserSpeedRequest) (*GetUserSpeedResponse, error) {
	egressBps, ingressBps := ratelimit.GetUserSpeed(request.GetEmail())
	return &GetUserSpeedResponse{
		Email:      request.GetEmail(),
		EgressBps:  egressBps,
		IngressBps: ingressBps,
	}, nil
}

func (s *rateLimitServer) ListUserSpeeds(ctx context.Context, request *ListUserSpeedsRequest) (*ListUserSpeedsResponse, error) {
	response := &ListUserSpeedsResponse{}
	for _, info := range ratelimit.ListUserSpeeds() {
		response.Infos = append(response.Infos, toProtoInfo(info))
	}
	return response, nil
}

func (s *rateLimitServer) mustEmbedUnimplementedRateLimitServiceServer() {}

type service struct{}

func (s *service) Register(server *grpc.Server) {
	rs := NewRateLimitServer()
	RegisterRateLimitServiceServer(server, rs)

	vCoreDesc := RateLimitService_ServiceDesc
	vCoreDesc.ServiceName = "v2ray.core.app.ratelimit.command.RateLimitService"
	server.RegisterService(&vCoreDesc, rs)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		if cfg == nil {
			return nil, errors.New("ratelimit command config is nil")
		}
		return &service{}, nil
	}))
}
