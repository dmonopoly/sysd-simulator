FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/sysd-simulator ./main.go

FROM alpine:3.20

RUN adduser -D -H appuser

WORKDIR /app

COPY --from=builder /bin/sysd-simulator /usr/local/bin/sysd-simulator

USER appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/sysd-simulator"]
