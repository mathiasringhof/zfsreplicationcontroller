FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/manager ./cmd/manager
RUN CGO_ENABLED=0 go build -o /out/zfsrep-sender ./cmd/zfsrep-sender
RUN CGO_ENABLED=0 go build -o /out/zfsrep-receiver ./cmd/zfsrep-receiver

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends zfsutils-linux ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/manager /usr/local/bin/manager
COPY --from=build /out/zfsrep-sender /usr/local/bin/zfsrep-sender
COPY --from=build /out/zfsrep-receiver /usr/local/bin/zfsrep-receiver
ENTRYPOINT ["/usr/local/bin/manager"]
