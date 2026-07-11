FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the relay binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /go/bin/relay ./cmd/relay

# Minimal runner image
FROM alpine:3.19

# Set working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /go/bin/relay /usr/local/bin/relay

# Run as non-root user
RUN adduser -D -g '' beamuser
USER beamuser

# Set default port
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["relay"]
