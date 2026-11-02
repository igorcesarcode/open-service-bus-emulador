# Shared access policies

[`Config.json`](Config.json) declares a namespace policy and two queue
policies (`producer` with Send, `consumer` with Listen). Run the emulator with
`-auth sas` so they are enforced:

```bash
open-service-bus -auth sas -config examples/auth-sas/Config.json   # terminal 1
cd examples/auth-sas && npm install && npm start                   # terminal 2
```

Output:

```
producer (Send): sent
producer (Send): receive refused: UnauthorizedAccess
consumer (Listen): received hello
wrong key: refused: UnauthorizedAccess
```

Other modes: `-auth none` (default) accepts every client; `-auth full` also
accepts Microsoft Entra ID tokens (`-dev-issuer` issues test tokens). See
[Usage](../../docs/usage.md#authentication).
