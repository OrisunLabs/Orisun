# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.1-bookworm AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files first for better layer caching
COPY go.mod go.sum ./

# Copy the source code
COPY . .

# Platform arguments for cross-compilation
ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

# Build arguments for version information
ARG VERSION=dev
ARG BUILD_TIME
ARG GIT_COMMIT
ARG TARGET_OS=linux
ARG TARGET_ARCH=amd64
ARG FLAVOR=pg

# Use target platform if available, otherwise use build args
RUN echo "Building for platform: $TARGETPLATFORM (OS: $TARGETOS, ARCH: $TARGETARCH)"

# Make build script executable and create build directory
RUN chmod +x ./build.sh && mkdir -p ./build

# Use build.sh to build the application with cross-compilation
# Use TARGETOS/TARGETARCH if available, otherwise fall back to TARGET_OS/TARGET_ARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    FINAL_OS=${TARGETOS:-${TARGET_OS}} && \
    FINAL_ARCH=${TARGETARCH:-${TARGET_ARCH}} && \
    echo "Building flavor: $FLAVOR with OS: $FINAL_OS, ARCH: $FINAL_ARCH" && \
    BUILD_TIME="${BUILD_TIME}" GIT_COMMIT="${GIT_COMMIT}" ./build.sh "$FINAL_OS" "$FINAL_ARCH" "${VERSION}" "${FLAVOR}"

# Verify the binary was created and list all files in build directory
RUN FINAL_OS=${TARGETOS:-${TARGET_OS}} && \
    FINAL_ARCH=${TARGETARCH:-${TARGET_ARCH}} && \
    case "$FLAVOR" in \
      pg|postgres) BINARY_NAME="orisun-pg-$FINAL_OS-$FINAL_ARCH" ;; \
      sqlite) BINARY_NAME="orisun-sqlite-$FINAL_OS-$FINAL_ARCH" ;; \
      *) echo "Unknown FLAVOR=$FLAVOR" && exit 1 ;; \
    esac && \
    ls -la ./build/ && \
    test -f "./build/$BINARY_NAME" && \
    echo "Binary $BINARY_NAME exists and is executable"

# Use a minimal Alpine image for the final container
FROM alpine:3.18

# Add CA certificates, timezone data, and su-exec for user switching
RUN apk add --no-cache ca-certificates tzdata su-exec

# Create a non-root user to run the application
RUN mkdir -p /app && adduser -D -h /app -s /sbin/nologin orisun
WORKDIR /app

# Platform arguments for the final stage
ARG TARGETOS
ARG TARGETARCH
ARG TARGET_OS=linux
ARG TARGET_ARCH=amd64
ARG FLAVOR=pg

# Copy the binary from the builder stage with dynamic architecture detection
COPY --from=builder /app/build/ /tmp/build/
RUN FINAL_OS=${TARGETOS:-${TARGET_OS}} && \
    FINAL_ARCH=${TARGETARCH:-${TARGET_ARCH}} && \
    case "$FLAVOR" in \
      pg|postgres) BINARY_NAME="orisun-pg-$FINAL_OS-$FINAL_ARCH" ;; \
      sqlite) BINARY_NAME="orisun-sqlite-$FINAL_OS-$FINAL_ARCH" ;; \
      *) echo "Unknown FLAVOR=$FLAVOR" && exit 1 ;; \
    esac && \
    echo "Looking for binary: $BINARY_NAME" && \
    ls -la /tmp/build/ && \
    if [ -f "/tmp/build/$BINARY_NAME" ]; then \
        cp "/tmp/build/$BINARY_NAME" /app/orisun; \
        echo "Successfully copied $BINARY_NAME to /app/orisun"; \
    else \
        echo "Binary $BINARY_NAME not found!" && \
        echo "Available binaries:" && \
        ls -la /tmp/build/ && \
        exit 1; \
    fi && \
    chmod +x /app/orisun && \
    ls -la /app/orisun && \
    echo "Binary /app/orisun is ready"

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set ownership and ensure data directory exists
RUN mkdir -p /var/lib/orisun/nats && chown -R orisun:orisun /var/lib/orisun

# Expose necessary ports (admin UI, gRPC, NATS, and additional gRPC port)
EXPOSE 8991 5005 4222 50051

# Set environment variables
ENV GO_ENV=production

# Declare volume for persistent data
VOLUME ["/var/lib/orisun/nats"]

# Set entrypoint and default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/app/orisun"]
