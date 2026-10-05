# One image with every StreamForge binary. Build context: repository root.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
# iptables: the fault-injection scripts cut links between store nodes with
# real packet filters (containers get CAP_NET_ADMIN in compose).
RUN apk add --no-cache iptables ca-certificates tzdata
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT []
