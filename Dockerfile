# ---- Builder Stage ----
FROM rust:latest AS builder
WORKDIR /app

# Set build-time arg variables
ARG BUILD_ENV=prod

# Set build-time environment variables
ENV BUILD_ENV=${BUILD_ENV}

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && mkdir src/bin && echo "fn main() {}" > src/bin/dummy.rs
RUN if [ "${BUILD_ENV}" = "dev" ]; then \
        echo ">>> Caching dependencies for DEV, adding [dev] feature..." && \
        cargo build --bin dummy --release --features dev; \
    else \
        echo ">>> Caching dependencies for PROD..." && \
        cargo build --bin dummy --release; \
    fi

# Remove the dummy src/ after building dependencies
RUN rm -rf src

# Copy the remaining application files
COPY src/ ./src/
COPY robots.txt ./
COPY config/ ./config/

# Build the application
RUN if [ "${BUILD_ENV}" = "dev" ]; then \
        echo ">>> Building application for DEV, adding [dev] feature..." && \
        cargo build --release --features dev; \
    else \
        echo ">>> Building application for PROD..." && \
        cargo build --release; \
    fi

# ---- Final Stage ----
FROM rust:slim AS final
WORKDIR /app

# Set runtime arg variables
ARG PORT=8080

# Set runtime environment variables
ENV PORT=${PORT}

# Copy the binary and necessary files from the builder stage
COPY --from=builder /app/target/release/rust-gcp .
COPY --from=builder /app/robots.txt ./
COPY --from=builder /app/config ./config

# Run the application
EXPOSE ${PORT}
CMD ["/app/rust-gcp"]