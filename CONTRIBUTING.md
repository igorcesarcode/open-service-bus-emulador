# Contributing

Thanks for helping. This project is an emulator of Azure Service Bus meant
for tests; compatibility with the official SDKs is the goal.

## Setup

```bash
git clone https://github.com/igorcesarcode/open-service-bus-emulador.git
cd open-service-bus-emulador
cd test/node && npm ci && cd ../..     # JavaScript suites
go build ./... && go vet ./...
go test -race ./...
```

Go 1.26 or newer is required. Node.js 22 is used by CI; the .NET SDK and
Docker are optional.

## Rules

- **Branches:** work on a branch off `develop` and open a pull request to
  `develop`. `main` receives merges from `develop`.
- **Style:** follow the [Google Go Style Guide](https://google.github.io/styleguide/go/);
  run `gofmt`.
- **Tests:** every new feature needs scenarios in **both** SDK suites: Go
  (`test/integration`) and JavaScript (`test/node`, run by `TestNodeSDK`).
  Protocol or storage changes also need unit tests next to the code.
  Behavior shared by the end-to-end services must keep
  `test/e2e/spec` and `test/node/e2e/spec.mjs` in sync.
- **Race detector:** `go test -race ./...` must pass.
- **No copied code:** do not copy or adapt code from other emulators or from
  Microsoft's repositories. Reading them for behavior is fine.
- **Language:** code, comments, documentation and commit messages in English.
- **License:** by contributing you agree that your contribution is licensed
  under the Apache License 2.0.

## Where things are

See [docs/how-it-works.md](docs/how-it-works.md) for an overview and
[docs/atom-wire-spec.md](docs/atom-wire-spec.md) for the management API wire format.
