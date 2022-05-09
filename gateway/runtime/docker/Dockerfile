FROM alpine:3.9

RUN apk add --no-cache ca-certificates

WORKDIR /

COPY datly .

EXPOSE 8080
ENTRYPOINT ["/datly", "-c", "$configURL"]
