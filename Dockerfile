# syntax=docker/dockerfile:1

# --- build stage: full Go toolchain ---
FROM golang:1.24-alpine AS build
WORKDIR /src
# Download modules first so this layer is cached when only code changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary, no cgo, debug info stripped.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/settlement ./cmd/settlement

# --- runtime stage: no shell, no package manager, non-root ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/settlement /settlement
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/settlement"]
