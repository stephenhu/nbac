FROM golang:latest as builder
WORKDIR /sources
COPY . .
RUN go build

FROM ubuntu:latest
LABEL org.opencontainers.image.source https://github.com/stephenhu/nbac
LABEL org.opencontainers.image.description="nba cli container image - nbac"
LABEL org.opencontainers.image.licenses=MIT
WORKDIR /usr/local/nbac
COPY --from=builder /sources/nbac .
CMD ["/usr/local/nbac/nbac", "pull", "nba"]
