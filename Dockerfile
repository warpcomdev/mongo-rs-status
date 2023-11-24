FROM golang:1.21 AS builder

WORKDIR /app
COPY go.mod go.sum main.go /app/
RUN  CGO_ENABLED=0 go build

FROM busybox:1.36
COPY --from=builder /app/mongo-rs-status /mongo-rs-status
ENTRYPOINT ["/mongo-rs-status"]
