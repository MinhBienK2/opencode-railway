# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o opencode-railway .

# Run stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/opencode-railway /app/opencode-railway

EXPOSE 8080

CMD ["/app/opencode-railway"]
