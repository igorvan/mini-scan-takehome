FROM golang:1.25.3 AS builder

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 go build -o processor ./cmd/processor

# Copy binary into slim image
FROM alpine
WORKDIR app
COPY --from=builder /src/processor .
CMD ["/app/processor"]
