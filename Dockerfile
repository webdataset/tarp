FROM golang:1.21-alpine

WORKDIR /src
# RUN apt update && apt install libzmq3-dev libczmq-dev -y
RUN apk add --no-cache libzmq czmq-dev git build-base
RUN git clone https://github.com/tmbdev/tarp.git
WORKDIR /src/tarp/tarp
RUN go clean
RUN go mod tidy
RUN go get -u
RUN CGO_ENABLED=1 go build -o tarp *.go
RUN cp tarp /bin

FROM alpine:latest

COPY --from=0 /bin/tarp /bin/tarp
RUN apk add --no-cache libzmq czmq-dev git
CMD ["/bin/tarp"]