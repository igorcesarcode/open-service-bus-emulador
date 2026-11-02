# GitHub Actions

[`integration-tests.yml`](integration-tests.yml) shows two ways to have the
emulator in a CI job: download the release binary and start it in the
background, or run the container image as a service. Set the version to a
release you use (`v1.0.0` is the first). While the repository is private, the
download and the image need a token; see the
[installation guide](../../docs/installation.md#download-a-release).

Tips: use `-log-level warn` to keep the logs short, wait for port 5672 before
starting the tests, and prefer a fresh in-memory emulator per job (no `-db`).
