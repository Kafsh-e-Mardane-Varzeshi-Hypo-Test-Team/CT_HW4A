FROM golang:alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN go build -o cmd/client/main ./cmd/client/main.go

CMD ["/app/cmd/client/main"]
