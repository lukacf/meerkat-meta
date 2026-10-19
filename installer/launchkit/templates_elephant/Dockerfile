# Built only from the exact source revision recorded in settings.json.
# The source declares Rust 1.94. Its own Dockerfile is a release build (pinned
# images, static ONNX Runtime, extra storage and embedding features); this one
# builds the default features.
FROM rust:1.94-bookworm AS build
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY . .
ENV CARGO_PROFILE_RELEASE_LTO=false
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
# The source's cargo config turns on incremental builds, which a one-off image build does not reuse.
ENV CARGO_INCREMENTAL=0
RUN cargo build --locked --release -p meerkat-recall-bin --features meerkat-extraction
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
RUN useradd -r -s /bin/false elephant && mkdir -p /app/assets && chown -R elephant:elephant /app
WORKDIR /app
COPY --from=build /app/target/release/meerkat-recall /usr/local/bin/meerkat-recall
# The Compose services run the binary under its former command name.
RUN ln -s meerkat-recall /usr/local/bin/elephant
COPY --from=build /app/crates/migrations/sql/ /app/migrations/
COPY --from=build /app/skills/ /app/skills/
USER elephant
ENV PIPELINE_SKILLS_DIR=/app/skills
ENV PIPELINE_MULTIMODAL_ASSET_ROOT=/app/assets
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=12 CMD curl -f http://localhost:3000/v1/health || exit 1
CMD ["elephant", "serve"]
