FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o broker ./cmd/broker

FROM alpine:3.21
RUN addgroup -S broker && adduser -S broker -G broker
RUN mkdir -p /data && chown broker:broker /data
USER broker
COPY --from=builder /app/broker /usr/local/bin/broker
EXPOSE 9092
ENTRYPOINT ["broker"]
