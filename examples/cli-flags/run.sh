#!/usr/bin/env sh
# Starts the emulator with a few entities. Requires open-service-bus on the PATH.
exec open-service-bus -queue orders,payments -topic events -subscription events/audit
