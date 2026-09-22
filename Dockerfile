# Global build args
ARG RUST_IMAGE=rust:1.95-alpine

# Stage 1: build frontend
# Use --platform=$BUILDPLATFORM to run on the native runner (fast)
FROM --platform=$BUILDPLATFORM node:24-alpine AS frontend

# Wealthfolio Connect configuration (baked into JS bundle at build time)
# Pass via --build-arg to enable; omit to build without Connect.
ARG CONNECT_AUTH_URL=
ARG CONNECT_AUTH_PUBLISHABLE_KEY=
ENV CONNECT_AUTH_URL=${CONNECT_AUTH_URL}
ENV CONNECT_AUTH_PUBLISHABLE_KEY=${CONNECT_AUTH_PUBLISHABLE_KEY}

WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY . .
ENV CI=1
ENV BUILD_TARGET=web
RUN npm install -g pnpm@9.9.0 && pnpm install --frozen-lockfile
# Build only the main app to avoid building workspace addons in this image
RUN pnpm --filter frontend... build && mv dist /web-dist

# Stage 2: build server with cross-compilation
FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS backend
# Copy xx scripts to handle cross-compilation
COPY --from=xx / /
ARG TARGETPLATFORM

# Wealthfolio Connect configuration (baked into server binary at build time)
ARG CONNECT_AUTH_URL=
ARG CONNECT_AUTH_PUBLISHABLE_KEY=
ENV CONNECT_AUTH_URL=${CONNECT_AUTH_URL}
ENV CONNECT_AUTH_PUBLISHABLE_KEY=${CONNECT_AUTH_PUBLISHABLE_KEY}

WORKDIR /app

# Install build tools for the HOST (to run cargo, build scripts)
# clang/lld are needed for cross-linking
# pkgconfig is required for openssl-sys to find the target libraries
# `perl` is required by the vendored OpenSSL that SQLCipher links against;
# `build-base` already provides make/gcc.
RUN apk add --no-cache clang lld build-base git file pkgconfig perl

# Install TARGET dependencies
# xx-apk installs into /$(xx-info triple)/...
RUN xx-apk add --no-cache musl-dev gcc openssl-dev openssl-libs-static sqlite-dev

# Install rust target
RUN rustup target add $(xx-cargo --print-target-triple)

# Leverage Docker layer caching for dependencies
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY apps/server ./apps/server
# Stub out apps/tauri so the workspace resolves (not built in Docker)
COPY apps/tauri/Cargo.toml apps/tauri/Cargo.toml
RUN mkdir -p apps/tauri/src && echo "fn main(){}" > apps/tauri/src/main.rs && echo "" > apps/tauri/src/lib.rs
RUN mkdir -p apps/server/src && \
    echo "fn main(){}" > apps/server/src/main.rs
# Cache mounts persist the registry download and compiled dependency
# artifacts across builds, independent of Docker layer invalidation (unlike
# the layer cache, a cache mount survives even when an earlier COPY's content
# changes). This is what actually saves time on a source-only change: without
# it, every rebuild recompiles the whole dependency graph from scratch.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-$TARGETPLATFORM \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-$TARGETPLATFORM \
    --mount=type=cache,target=/app/target,id=cargo-target-$TARGETPLATFORM \
    xx-cargo fetch --locked --manifest-path apps/server/Cargo.toml

# Now copy full sources
COPY crates ./crates
COPY apps/server ./apps/server
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV OPENSSL_STATIC=1
ENV CARGO_BUILD_JOBS=2
# Build using xx-cargo which handles target flags. Same cache mounts as the
# fetch step above, so dependency builds carry over between images.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-$TARGETPLATFORM \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-$TARGETPLATFORM \
    --mount=type=cache,target=/app/target,id=cargo-target-$TARGETPLATFORM \
    xx-cargo build --locked --release --manifest-path apps/server/Cargo.toml && \
    # Move the binary out of the cache-mounted target dir to a predictable,
    # persisted location (the mount disappears once the RUN step ends).
    cp target/$(xx-cargo --print-target-triple)/release/wealthfolio-server /wealthfolio-server

# Final stage
FROM alpine:3.19
WORKDIR /app
# Copy from backend (which is now build platform, but binary is target platform)
COPY --from=backend /wealthfolio-server /usr/local/bin/wealthfolio-server
COPY --from=frontend /web-dist ./dist
ENV WF_DB_PATH=/data/wealthfolio.db
# Wealthfolio Connect API URL (can be overridden at runtime via -e or docker-compose)
ARG CONNECT_API_URL=
ENV CONNECT_API_URL=${CONNECT_API_URL}

# Run as non-root. chown /data BEFORE the VOLUME directive so named volumes
# inherit ownership on first creation. Existing volumes from older images
# need a one-time chown — see docs/self-host/README.md.
RUN addgroup -S -g 1000 wealthfolio \
 && adduser -S -u 1000 -G wealthfolio -H -s /sbin/nologin wealthfolio \
 && mkdir -p /data \
 && chown -R wealthfolio:wealthfolio /data
USER 1000:1000

EXPOSE 8088
CMD ["/usr/local/bin/wealthfolio-server"]
