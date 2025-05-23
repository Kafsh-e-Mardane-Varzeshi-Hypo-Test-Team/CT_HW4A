FROM golang:alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY ./internal/cluster/ .

RUN go build -o /app/cmd/main ./cmd/main.go

EXPOSE 3001

CMD [ "/app/cmd/main" ]
