FROM registry.ddbuild.io/dd-octo-sts:v1.10.6@sha256:2b1a69019340da2be0e1e71de4d827acee2fdc5ee38f676dd96aa88c112ed6e8 AS dd-octo-sts

FROM registry.ddbuild.io/images/mirror/rust:1.85

# System dependencies + gh CLI (for gh release create in publish job)
RUN apt-get update && apt-get install -y \
    curl \
    pkg-config \
    libsystemd-dev \
    libdbus-glib-1-dev \
    build-essential \
    libelf-dev \
    libseccomp-dev \
    libclang-dev \
    libzstd-dev \
    libssl-dev \
    protobuf-compiler \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

# dd-octo-sts for GitHub token issuance via OIDC (Datadog CI pattern)
COPY --from=dd-octo-sts /usr/local/bin/dd-octo-sts /usr/local/bin/dd-octo-sts

# nightly toolchain for rustfmt + wasm32 target
RUN rustup toolchain install nightly --component rustfmt \
    && rustup target add wasm32-wasip2

