FROM golang:1.24.0 AS builder

WORKDIR /src

ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.org

ENV GOPROXY=https://goproxy.cn,direct
ENV GOSUMDB=${GOSUMDB}

COPY go.mod ./go.mod
COPY go.sum ./go.sum
WORKDIR /src
RUN go mod download

COPY . .
WORKDIR /src
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/client-demo .

FROM debian:bookworm-slim

WORKDIR /app

RUN mkdir -p /app/logs

COPY --from=builder /out/client-demo /app/client-demo

EXPOSE 18082 12114

ENV APP_LOG_PATH=/app/logs/client-demo.log

CMD ["/app/client-demo"]
