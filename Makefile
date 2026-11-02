# Builds open-service-bus. The binary is pure Go (CGO_ENABLED=0), so it
# cross-compiles for every platform below from any host.

VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64
DIST := dist

.PHONY: build release checksums image image-binaries test clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(DIST)/open-service-bus ./cmd/open-service-bus

release:
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; ext=; \
		[ "$$os" = windows ] && ext=.exe; \
		out=$(DIST)/open-service-bus-$(VERSION)-$$os-$$arch$$ext; \
		echo "building $$out"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags '$(LDFLAGS)' -o $$out ./cmd/open-service-bus || exit 1; \
	done

# checksums writes dist/SHA256SUMS for the files of a release; verify with
# `sha256sum -c SHA256SUMS` inside dist.
checksums:
	cd $(DIST) && sha256sum open-service-bus-* > SHA256SUMS

# image-binaries builds the Linux binaries the container image holds, one per
# architecture, and compresses them with UPX when it is installed (the image
# has no use for the unpacked file, and the cost is a slower start of about
# a tenth of a second). The binaries of a release are not compressed.
IMAGEDIR := $(DIST)/image
image-binaries:
	mkdir -p $(IMAGEDIR)
	@for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -trimpath -ldflags '$(LDFLAGS)' -o $(IMAGEDIR)/open-service-bus-$$arch ./cmd/open-service-bus || exit 1; \
	done
	@if command -v upx >/dev/null 2>&1; then upx --best --lzma -q $(IMAGEDIR)/open-service-bus-amd64 $(IMAGEDIR)/open-service-bus-arm64; \
	else echo "upx not found: the image keeps the uncompressed binary"; fi
	cp Dockerfile $(IMAGEDIR)/Dockerfile

# image builds a container image for this machine's architecture. It has the
# binary alone (FROM scratch), so give it a volume for -db.
IMAGE ?= open-service-bus:$(VERSION)
image: image-binaries
	docker build -t $(IMAGE) $(IMAGEDIR)

test:
	go test -race ./...

clean:
	rm -rf $(DIST)
