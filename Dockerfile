FROM golang:alpine as builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o ec2-price-exporter .

FROM alpine:3.17

WORKDIR /app

COPY --from=builder /app/ec2-price-exporter /bin

ENTRYPOINT ["/bin/ec2-price-exporter"]
