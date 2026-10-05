FROM rust:1-bookworm AS build
WORKDIR /app
# Build dependencies first so code changes don't rebuild them.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src
COPY src src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      chromium xvfb tini ca-certificates \
      fonts-liberation fonts-noto-color-emoji fonts-noto-cjk \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/notebook /usr/local/bin/notebook
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["tini", "--"]
CMD ["notebook"]
