FROM golang:latest as builder
WORKDIR /sources
COPY . .
RUN go build

FROM ubuntu:latest
WORKDIR /usr/local/nbac
COPY --from=builder /sources/nbac .
CMD ["/usr/local/nbac/nbac", "pull", "nba"]
