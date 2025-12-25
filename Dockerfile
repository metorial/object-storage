# Build stage
FROM rust:1.91-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY object-store/Cargo.toml ./object-store/
COPY object-store-backends/Cargo.toml ./object-store-backends/

# Create dummy source files to cache dependencies
RUN mkdir -p object-store/src object-store-backends/src && \
    echo "fn main() {}" > object-store/src/main.rs && \
    echo "pub fn dummy() {}" > object-store/src/lib.rs && \
    echo "pub fn dummy() {}" > object-store-backends/src/lib.rs && \
    cargo build --release && \
    rm -rf object-store/src object-store-backends/src

# Copy actual source code
COPY object-store ./object-store
COPY object-store-backends ./object-store-backends

# Remove the cached dummy binaries to force rebuild
RUN rm -rf target/release/object-store-service target/release/deps/object_store_service* target/release/.fingerprint/object-store-*

# Build the actual binary
RUN cargo build --release --bin object-store-service

# Runtime stage
FROM debian:trixie-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN apt install -y curl

# Create a non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/object-store-service /app/object-store-service

# Create data directory for local backend
RUN mkdir -p /app/data

# Change ownership
RUN chown -R appuser:appuser /app

USER appuser

# Volumes for persistent data and configuration
VOLUME /app/data
VOLUME /app/config

# Expose the port (default is 8080, configurable via config or env vars)
EXPOSE 8080

# Set environment variables
ENV RUST_LOG=info
# CONFIG_PATH should be set at runtime or config provided via environment variables

# Run the binary
CMD ["/app/object-store-service"]
