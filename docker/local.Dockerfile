FROM golang:1.27-alpine3.24 AS build

RUN apk add --no-cache git

WORKDIR /s
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN go generate ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mediamtx .

FROM scratch

COPY --from=build /out/mediamtx /mediamtx
COPY mediamtx.yml /mediamtx.yml
COPY LICENSE /LICENSE

EXPOSE 8554 1935 8888 8889 8890/udp 8189/udp 9997

ENTRYPOINT ["/mediamtx"]
