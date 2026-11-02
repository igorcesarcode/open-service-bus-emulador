# GitHub Actions

[`integration-tests.yml`](integration-tests.yml) shows two ways to have the
emulator in a CI job: download the release binary and start it in the
background, or run the container image as a service. Version numbers and the
image name are placeholders until the first release and image are published.

Tips: use `-log-level warn` to keep the logs short, wait for port 5672 before
starting the tests, and prefer a fresh in-memory emulator per job (no `-db`).
