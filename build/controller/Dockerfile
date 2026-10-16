# Build stage: always runs on the host platform for speed (cross-compiles via GOARCH)
FROM --platform=$BUILDPLATFORM golang:1.18 AS builder

WORKDIR /go/src/sigs.k8s.io/appgroup-controller
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-w" \
    -o bin/controller cmd/controller/controller.go

# Final stage: uses the correct arch image automatically via buildx
FROM alpine:3.16

COPY --from=builder /go/src/sigs.k8s.io/appgroup-controller/bin/controller /bin/controller

WORKDIR /bin
CMD ["controller"]
