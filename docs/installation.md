# Installation

open-service-bus is a single static binary named `open-service-bus`
(`open-service-bus.exe` on Windows). It has no runtime dependencies.

| OS | Architectures |
| --- | --- |
| Linux | amd64, arm64 |
| macOS | amd64 (Intel), arm64 (Apple Silicon) |
| Windows | amd64, arm64 |

## Download a release

Releases are at <https://github.com/igorcesarcode/open-service-bus-emulador/releases>.
Each one contains one binary per platform and a `SHA256SUMS` file.

> While the repository is private, `curl` gets a 404 for the download URLs.
> Use the GitHub CLI, which is logged in: `gh release download v1.0.0 -R
> igorcesarcode/open-service-bus-emulador -p 'open-service-bus-v1.0.0-linux-amd64'`
> (then `chmod +x`), or `-p '*'` for everything including `SHA256SUMS`.

Release assets are named `open-service-bus-<version>-<os>-<arch>[.exe]`.

### Linux

```bash
VERSION=v1.0.0   # pick a version from the releases page
ARCH=amd64       # or arm64
curl -L -o open-service-bus \
  "https://github.com/igorcesarcode/open-service-bus-emulador/releases/download/${VERSION}/open-service-bus-${VERSION}-linux-${ARCH}"
chmod +x open-service-bus
sudo mv open-service-bus /usr/local/bin/      # optional
open-service-bus -version
```

### macOS

```bash
VERSION=v1.0.0
ARCH=arm64       # arm64 for Apple Silicon, amd64 for Intel
curl -L -o open-service-bus \
  "https://github.com/igorcesarcode/open-service-bus-emulador/releases/download/${VERSION}/open-service-bus-${VERSION}-darwin-${ARCH}"
chmod +x open-service-bus
xattr -d com.apple.quarantine open-service-bus 2>/dev/null   # if Gatekeeper blocks it
./open-service-bus -version
```

The binaries are not signed or notarized. If macOS refuses to open the file,
remove the quarantine attribute as above (or allow it in *System Settings,
Privacy & Security*).

### Windows (PowerShell)

```powershell
$Version = "v1.0.0"
$Arch = "amd64"       # or arm64
Invoke-WebRequest -OutFile open-service-bus.exe `
  "https://github.com/igorcesarcode/open-service-bus-emulador/releases/download/$Version/open-service-bus-$Version-windows-$Arch.exe"
.\open-service-bus.exe -version
```

Windows may ask to allow the program through the firewall the first time it
listens on a port. Allowing private networks is enough, and binding to
`localhost` only is possible with `-listen 127.0.0.1:5672`.

### Verify the download

```bash
sha256sum -c SHA256SUMS --ignore-missing      # Linux
shasum -a 256 -c SHA256SUMS --ignore-missing  # macOS
```

```powershell
(Get-FileHash .\open-service-bus.exe -Algorithm SHA256).Hash   # compare with SHA256SUMS
```

## Container image

The image is published on the GitHub Container Registry as
`ghcr.io/igorcesarcode/open-service-bus-emulador`: `latest` follows `main`, and
each release has its version (`1.0.0`, `1.0`) for linux/amd64 and linux/arm64.
If the package is private, log in first with a token that has `read:packages`:
`echo $TOKEN | docker login ghcr.io -u <user> --password-stdin`.

The image holds only the static binary (`FROM scratch`) and exposes port 5672,
which carries AMQP, AMQP over WebSocket, the management API and the portal.
Clients that connect **without** `UseDevelopmentEmulator` and name no port dial
5671 (and 443 for the management API); see
[Connecting without `UseDevelopmentEmulator`](usage.md#connecting-without-usedevelopmentemulator).
Configure it with `OSB_*` environment variables (every flag has one; see
[Configuration](configuration.md#environment-variables)) or with arguments.

```bash
# entities from variables
docker run --rm -p 5672:5672 \
  -e OSB_QUEUE=orders,payments -e OSB_TOPIC=events -e OSB_SUBSCRIPTION=events/audit \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest

# the same with arguments
docker run --rm -p 5672:5672 ghcr.io/igorcesarcode/open-service-bus-emulador:latest \
  -queue orders,payments -topic events -subscription events/audit

# entities from a file, state kept in a volume
docker run --rm -p 5672:5672 \
  -v osb-data:/data -v "$PWD/Config.json:/config/Config.json:ro" \
  -e OSB_CONFIG=/config/Config.json -e OSB_DB=/data/osb.db \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest
```

Until the image is published you can build it yourself (needs Go and Docker):

```bash
make image IMAGE=open-service-bus:local
docker run --rm -p 5672:5672 -e OSB_QUEUE=orders open-service-bus:local
```

See [`examples/docker`](../examples/docker) for Compose files covering the
emulator alone, with an application, from Terraform, with authentication and
with the MCP endpoint.

## Build from source

You need [Go](https://go.dev/dl/) 1.26 or newer (see `go.mod`). No C compiler
is needed: the SQLite driver is pure Go.

```bash
git clone https://github.com/igorcesarcode/open-service-bus-emulador.git
cd open-service-bus-emulador
make build            # writes dist/open-service-bus
# or, without make:
go build -o open-service-bus ./cmd/open-service-bus
```

Or install straight from the module:

```bash
go install github.com/igorcesarcode/open-service-bus-emulador/cmd/open-service-bus@latest
```

Cross-compile every platform with `make release checksums` (output in
`dist/`).

## Check that it works

```bash
open-service-bus -queue demo
```

The log prints the connection string. Open <https://localhost:5672/$portal/>
(accept the self-signed certificate) to see the portal with the `demo` queue.
Stop the emulator with `Ctrl+C`; it shuts down gracefully.

## Next

- [Usage guide](usage.md)
- [Examples](../examples/README.md)
