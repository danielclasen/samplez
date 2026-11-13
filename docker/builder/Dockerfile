# Build environment for samplez: Rust, clang and libclang for bindgen, and a
# Release build of libmxl (MXL v1.1.0) installed under /opt/mxl.
FROM debian:trixie-20261027@sha256:2dc419c6d7fe3dad0a2f9fd469c34cc90b0f9ba642d8fde837fe3d7d34a0e7f1 AS libmxl

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# MXL v1.1.0
ARG MXL_SHA=8f3f1096e804bfb822eb959cdf2fc23199a7d15d
# vcpkg tool and registry checkout. Port versions follow the
# builtin-baseline and minimum versions in libmxl's vcpkg.json.
ARG VCPKG_REF=3d22b64585ca026e881728fd87e541c23a611a43

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential clang lld cmake ninja-build git curl zip unzip tar \
      pkg-config ca-certificates python3 linux-libc-dev autoconf automake \
      libtool bison flex nasm \
 && rm -rf /var/lib/apt/lists/*

# The upstream CMake presets expect vcpkg at ~/vcpkg.
RUN git clone https://github.com/microsoft/vcpkg.git /root/vcpkg \
 && git -C /root/vcpkg checkout -q "${VCPKG_REF}" \
 && /root/vcpkg/bootstrap-vcpkg.sh -disableMetrics

RUN git init -q /src/mxl \
 && git -C /src/mxl fetch -q --depth 1 https://github.com/dmf-mxl/mxl.git "${MXL_SHA}" \
 && git -C /src/mxl checkout -q FETCH_HEAD
WORKDIR /src/mxl
RUN cmake --preset Linux-Clang-Release \
      -DBUILD_DOCS=OFF -DBUILD_TESTS=OFF -DBUILD_TOOLS=ON -DBUILD_UTILS=OFF \
      -DCMAKE_INSTALL_PREFIX=/opt/mxl -DCMAKE_INSTALL_LIBDIR=lib \
 && cmake --build build/Linux-Clang-Release \
 && cmake --install build/Linux-Clang-Release \
 && install -Dm0644 LICENSE.txt /opt/mxl/share/licenses/mxl/LICENSE.txt \
 && cp -r LICENSES /opt/mxl/share/licenses/mxl/LICENSES \
 && for d in build/Linux-Clang-Release/vcpkg_installed/x64-linux/share/*/; do \
      if [ -f "${d}copyright" ]; then \
        install -Dm0644 "${d}copyright" "/opt/mxl/share/licenses/$(basename "${d}")/copyright"; \
      fi; \
    done

FROM debian:trixie-20261027@sha256:2dc419c6d7fe3dad0a2f9fd469c34cc90b0f9ba642d8fde837fe3d7d34a0e7f1

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

ARG RUST_VERSION=1.96.0
ARG RUSTUP_VERSION=1.29.0
ARG RUSTUP_SHA256=4acc9acc76d5079515b46346a485974457b5a79893cfb01112423c89aeb5aa10

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential clang libclang-dev pkg-config ca-certificates curl git \
 && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSfo /tmp/rustup-init \
      "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/x86_64-unknown-linux-gnu/rustup-init" \
 && echo "${RUSTUP_SHA256}  /tmp/rustup-init" | sha256sum -c - \
 && chmod +x /tmp/rustup-init \
 && /tmp/rustup-init -y --no-modify-path --profile minimal \
      --default-toolchain "${RUST_VERSION}" --component clippy,rustfmt \
 && rm /tmp/rustup-init \
 && chmod -R a+w "${RUSTUP_HOME}" "${CARGO_HOME}" \
 && rustc -V && cargo -V && cargo clippy -V && cargo fmt --version

COPY --from=libmxl /opt/mxl /opt/mxl
RUN echo /opt/mxl/lib > /etc/ld.so.conf.d/mxl.conf && ldconfig
ENV PATH=/opt/mxl/bin:$PATH
