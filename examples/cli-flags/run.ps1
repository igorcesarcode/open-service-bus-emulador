# Starts the emulator with a few entities. Requires open-service-bus.exe on the PATH.
open-service-bus.exe -queue orders,payments -topic events -subscription events/audit
