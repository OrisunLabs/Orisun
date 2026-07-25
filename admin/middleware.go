package admin

import (
	"context"
	"encoding/base64"
	"log"
	"strings"
	"time"

	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// middleware to measure performance of grpc requests
func UnaryPerformanceInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)
		log.Printf("Unary request %s took %v", info.FullMethod, elapsed)
		return resp, err
	}
}

func UnaryAuthInterceptor(auth *Authenticator, logger l.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		user, token, err := authenticate(ctx, auth, logger)
		if err != nil {
			return nil, err
		}

		withUserCtx := context.WithValue(ctx, orisun.UserContextKey, user)

		// Set the token in response headers using grpc.SetHeader
		if err := grpc.SetHeader(withUserCtx, metadata.Pairs(tokenHeaderName, token)); err != nil {
			logger.Errorf("Failed to set token header: %v", err)
		}

		if auth.logger.IsDebugEnabled() {
			auth.logger.Debugf("Authenticated user %s for method %s", user.Username, info.FullMethod)
		}
		return handler(withUserCtx, req)
	}
}

func StreamAuthInterceptor(auth *Authenticator, logger l.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		user, token, err := authenticate(ss.Context(), auth, logger)
		if err != nil {
			return err
		}

		newCtx := context.WithValue(ss.Context(), orisun.UserContextKey, user)

		// Set the token in response headers using grpc.SetHeader
		if err := grpc.SetHeader(newCtx, metadata.Pairs(tokenHeaderName, token)); err != nil {
			logger.Errorf("Failed to set token header: %v", err)
		}

		wrapped := newWrappedServerStream(ss, newCtx)
		return handler(srv, wrapped)
	}
}

const tokenHeaderName = "x-auth-token"

func authenticate(ctx context.Context, auth *Authenticator, logger l.Logger) (orisun.User, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	// get token if present in header
	token := md.Get(tokenHeaderName)
	if len(token) > 0 {
		user, err := auth.ValidateToken(ctx, token[len(token)-1])
		if err == nil {
			return *user, token[len(token)-1], nil
		}

		logger.Errorf("invalid token: %v", err)
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := values[0]
	if !strings.HasPrefix(authHeader, "Basic ") {
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "invalid authorization header format")
	}

	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "invalid authorization header encoding")
	}

	// logger.Infof( "authenticate %v %v", "authHeader", authHeader)
	credentials := strings.SplitN(string(decoded), ":", 2)
	if len(credentials) != 2 {
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "invalid authorization header format")
	}

	user, generatedToken, err := auth.ValidateCredentials(ctx, credentials[0], credentials[1])

	if err != nil {
		auth.logger.Error(ctx, err.Error())
		return orisun.User{}, "", status.Error(codes.Unauthenticated, "invalid credentials")
	}

	return user, generatedToken, nil
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func newWrappedServerStream(ss grpc.ServerStream, ctx context.Context) grpc.ServerStream {
	return &wrappedServerStream{
		ServerStream: ss,
		ctx:          ctx,
	}
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
