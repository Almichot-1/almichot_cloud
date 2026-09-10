FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/control-plane ./cmd/control-plane

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 nebula

USER nebula
COPY --from=build /out/control-plane /usr/local/bin/control-plane

ENV NEBULA_HTTP_ADDR=:8081 \
    NEBULA_GRPC_ADDR=:9090

EXPOSE 8081 9090
ENTRYPOINT ["control-plane"]