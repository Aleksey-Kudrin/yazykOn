FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/yazykon-sfu .

FROM debian:bookworm-slim
RUN groupadd --system yazykon && useradd --system --gid yazykon --no-create-home yazykon
COPY --from=build /out/yazykon-sfu /usr/local/bin/yazykon-sfu
RUN chown yazykon:yazykon /usr/local/bin/yazykon-sfu
USER yazykon
EXPOSE 4000
ENTRYPOINT ["/usr/local/bin/yazykon-sfu"]
