FROM docker.io/library/golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/manager ./cmd/manager
RUN CGO_ENABLED=0 go build -o /out/zfsrep-sender ./cmd/zfsrep-sender

FROM docker.io/library/ubuntu:24.04
ARG SANOID_VERSION=2.3.0
ARG SANOID_SHA256=1d8735a271a34ec87ea46313a66f6f20bd38b583886924574d3c1f72ea173620
RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		curl \
		libcapture-tiny-perl \
		lzop \
		mbuffer \
		openssh-client \
		openssh-server \
		perl \
		pv \
		zfsutils-linux \
	&& curl -fsSL "https://github.com/jimsalterjrs/sanoid/archive/refs/tags/v${SANOID_VERSION}.tar.gz" -o /tmp/sanoid.tar.gz \
	&& printf '%s  /tmp/sanoid.tar.gz\n' "${SANOID_SHA256}" | sha256sum -c - \
	&& tar -xzf /tmp/sanoid.tar.gz -C /tmp \
	&& install -m 0755 "/tmp/sanoid-${SANOID_VERSION}/syncoid" /usr/local/bin/syncoid \
	&& syncoid --version | grep -F "${SANOID_VERSION}" \
	&& syncoid --help 2>&1 | grep -F -- "--include-snaps" \
	&& rm -rf /tmp/sanoid.tar.gz "/tmp/sanoid-${SANOID_VERSION}" /var/lib/apt/lists/*
COPY --from=build /out/manager /usr/local/bin/manager
COPY --from=build /out/zfsrep-sender /usr/local/bin/zfsrep-sender
COPY hack/zfsrep-ssh-receiver /usr/local/bin/zfsrep-ssh-receiver
RUN chmod +x /usr/local/bin/zfsrep-ssh-receiver
ENTRYPOINT ["/usr/local/bin/manager"]
