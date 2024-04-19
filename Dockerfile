FROM golang:1.22-alpine

RUN apk add git

WORKDIR /app
RUN git clone "https://github.com/hauke96/simple-osm-queries" .

RUN echo 0
RUN go mod download
RUN go build -o ./server .

ENTRYPOINT ["go", "run", "."]