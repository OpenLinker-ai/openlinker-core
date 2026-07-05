package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	a2apb.UnimplementedA2AServiceServer
	client a2apb.A2AServiceClient
	tenant string
	token  string
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:0", "address for the unauthenticated local TCK gRPC proxy")
	target := flag.String("target", "", "authenticated OpenLinker A2A gRPC target")
	tenant := flag.String("tenant", "", "A2A tenant/agent slug to inject when the TCK request omits tenant")
	token := flag.String("token", "", "OpenLinker bearer token to inject upstream")
	flag.Parse()

	if strings.TrimSpace(*target) == "" || strings.TrimSpace(*tenant) == "" || strings.TrimSpace(*token) == "" {
		fmt.Fprintln(os.Stderr, "-target, -tenant and -token are required")
		os.Exit(2)
	}

	conn, err := grpc.NewClient(grpcTarget(*target), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "create upstream grpc client: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", *listenAddr, err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	a2apb.RegisterA2AServiceServer(grpcServer, &server{
		client: a2apb.NewA2AServiceClient(conn),
		tenant: strings.TrimSpace(*tenant),
		token:  strings.TrimSpace(*token),
	})
	fmt.Fprintf(os.Stderr, "a2a-tck-grpc-proxy listening on %s -> %s tenant=%s\n", lis.Addr().String(), grpcTarget(*target), strings.TrimSpace(*tenant))
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "serve grpc proxy: %v\n", err)
		os.Exit(1)
	}
}

func grpcTarget(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "http://")
	value = strings.TrimPrefix(value, "https://")
	return strings.TrimRight(value, "/")
}

func (s *server) outgoingContext(ctx context.Context) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+s.token)
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		copied := incoming.Copy()
		if len(copied.Get("a2a-version")) == 0 {
			copied.Set("a2a-version", "1.0")
		}
		md = metadata.Join(copied, md)
	} else {
		md.Set("a2a-version", "1.0")
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func (s *server) injectTenant(raw string) string {
	if strings.TrimSpace(raw) != "" {
		return raw
	}
	return s.tenant
}

func (s *server) SendMessage(ctx context.Context, req *a2apb.SendMessageRequest) (*a2apb.SendMessageResponse, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.SendMessage(s.outgoingContext(ctx), req)
}

func (s *server) SendStreamingMessage(req *a2apb.SendMessageRequest, stream a2apb.A2AService_SendStreamingMessageServer) error {
	req.Tenant = s.injectTenant(req.GetTenant())
	upstream, err := s.client.SendStreamingMessage(s.outgoingContext(stream.Context()), req)
	if err != nil {
		return err
	}
	for {
		item, err := upstream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(item); err != nil {
			return err
		}
	}
}

func (s *server) GetTask(ctx context.Context, req *a2apb.GetTaskRequest) (*a2apb.Task, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.GetTask(s.outgoingContext(ctx), req)
}

func (s *server) ListTasks(ctx context.Context, req *a2apb.ListTasksRequest) (*a2apb.ListTasksResponse, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.ListTasks(s.outgoingContext(ctx), req)
}

func (s *server) CancelTask(ctx context.Context, req *a2apb.CancelTaskRequest) (*a2apb.Task, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.CancelTask(s.outgoingContext(ctx), req)
}

func (s *server) SubscribeToTask(req *a2apb.SubscribeToTaskRequest, stream a2apb.A2AService_SubscribeToTaskServer) error {
	req.Tenant = s.injectTenant(req.GetTenant())
	upstream, err := s.client.SubscribeToTask(s.outgoingContext(stream.Context()), req)
	if err != nil {
		return err
	}
	for {
		item, err := upstream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(item); err != nil {
			return err
		}
	}
}

func (s *server) CreateTaskPushNotificationConfig(ctx context.Context, req *a2apb.TaskPushNotificationConfig) (*a2apb.TaskPushNotificationConfig, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.CreateTaskPushNotificationConfig(s.outgoingContext(ctx), req)
}

func (s *server) GetTaskPushNotificationConfig(ctx context.Context, req *a2apb.GetTaskPushNotificationConfigRequest) (*a2apb.TaskPushNotificationConfig, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.GetTaskPushNotificationConfig(s.outgoingContext(ctx), req)
}

func (s *server) ListTaskPushNotificationConfigs(ctx context.Context, req *a2apb.ListTaskPushNotificationConfigsRequest) (*a2apb.ListTaskPushNotificationConfigsResponse, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.ListTaskPushNotificationConfigs(s.outgoingContext(ctx), req)
}

func (s *server) GetExtendedAgentCard(ctx context.Context, req *a2apb.GetExtendedAgentCardRequest) (*a2apb.AgentCard, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.GetExtendedAgentCard(s.outgoingContext(ctx), req)
}

func (s *server) DeleteTaskPushNotificationConfig(ctx context.Context, req *a2apb.DeleteTaskPushNotificationConfigRequest) (*emptypb.Empty, error) {
	req.Tenant = s.injectTenant(req.GetTenant())
	return s.client.DeleteTaskPushNotificationConfig(s.outgoingContext(ctx), req)
}
