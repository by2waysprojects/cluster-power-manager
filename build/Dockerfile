ARG BASE_IMAGE=scratch
ARG BUILDER_IMAGE=quay.io/projectquay/golang:1.26

# Build the manager binary
FROM --platform=$BUILDPLATFORM ${BUILDER_IMAGE} AS builder
ARG TARGETOS \
    TARGETARCH

# Go toolchain settings, consumed directly by the go command. The defaults
# produce a static binary suitable for the default scratch BASE_IMAGE.
# Overriding CGO_ENABLED=1 yields a dynamically linked binary and requires a
# BASE_IMAGE that provides a dynamic loader and the needed shared libraries.
ARG CGO_ENABLED=0
ARG GOFLAGS
ARG GOEXPERIMENT
ARG GOFIPS140

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# Bring in the go dependencies before anything else so we can take
# advantage of caching these layers in future builds.
COPY vendor/ vendor/
# Copy the go source
COPY build/manager/main.go main.go
COPY api/ api/
COPY controllers/ controllers/
COPY pkg/ pkg/
COPY internal/ internal/

# Build
RUN GO111MODULE=on GOOS=${TARGETOS:-linux} GOARCH=$TARGETARCH go build -a -o manager main.go

# Final image
FROM ${BASE_IMAGE}
ARG MANIFEST=build/manifests/power-node-agent-ds.yaml
WORKDIR /
COPY LICENSE /licenses/LICENSE
COPY --from=builder /workspace/manager .
COPY ${MANIFEST} /power-manifests/
USER 10001

ENTRYPOINT ["/manager"]
