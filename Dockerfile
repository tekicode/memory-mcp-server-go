FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.version=$(cat VERSION)" \
    -o /mms .

FROM scratch
COPY --from=builder /mms /mms
EXPOSE 8080
ENTRYPOINT ["/mms"]
