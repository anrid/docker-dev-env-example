FROM golang:1.17 AS build
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0
RUN go build -o server .

FROM build AS development
RUN apt-get update \
    && apt-get install -y git \
    && apt-get install -y protobuf-compiler
CMD ["go", "run", "main.go"]

FROM alpine:3.16
EXPOSE 8000
COPY --from=build /build/server /server
CMD ["/server"]
