# One image with every StreamForge binary and the built dashboard.
# Build context: repository root.

FROM golang:1.26-alpine AS go
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... \
 && GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o /out/sim.wasm ./cmd/simwasm \
 && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" /out/wasm_exec.js

FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
COPY --from=go /out/sim.wasm /out/wasm_exec.js ./public/
RUN npm run build

FROM alpine:3.22
# iptables: the fault-injection scripts cut links between store nodes with
# real packet filters (containers get CAP_NET_ADMIN in compose).
RUN apk add --no-cache iptables ca-certificates tzdata
COPY --from=go /out/ /usr/local/bin/
RUN rm -f /usr/local/bin/sim.wasm /usr/local/bin/wasm_exec.js
COPY --from=web /web/dist /srv/web
ENTRYPOINT []
