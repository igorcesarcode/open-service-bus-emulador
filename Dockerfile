# The image holds the static binary alone. `make image` builds it from
# dist/image, where the binaries of both architectures are placed next to this
# file.
FROM scratch
ARG TARGETARCH
COPY open-service-bus-${TARGETARCH} /open-service-bus
# AMQP and HTTP share one port. Keep the database on a volume:
#   docker run -p 5672:5672 -v osb:/data open-service-bus -db /data/osb.db
EXPOSE 5672
VOLUME /data
ENTRYPOINT ["/open-service-bus"]
CMD ["-listen", ":5672"]
