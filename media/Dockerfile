FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/yazykon-sfu .

FROM debian:bookworm-slim
COPY --from=build /out/yazykon-sfu /usr/local/bin/yazykon-sfu
EXPOSE 4000
ENTRYPOINT ["/usr/local/bin/yazykon-sfu"]
