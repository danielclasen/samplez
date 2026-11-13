# Build environment for samplez and the products: Rust, clang/libclang for
# bindgen, and a Release build of libmxl installed under /opt/mxl.
FROM debian:trixie-20261027 AS libmxl

ARG MXL_REF=v1.1.0
ARG VCPKG_REF=3d22b64585ca026e881728fd87e541c23a611a43

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential clang lld cmake ninja-build git curl zip unzip tar \
      pkg-config ca-certificates python3 linux-libc-dev autoconf automake \
      libtool bison flex nasm \
 && rm -rf /var/lib/apt/lists/*

# The upstream CMake presets expect vcpkg at ~/vcpkg. VCPKG_REF pins the
# vcpkg port versions used for libmxl dependencies.
RUN git clone https://github.com/microsoft/vcpkg.git /root/vcpkg \
 && git -C /root/vcpkg checkout -q "${VCPKG_REF}" \
 && /root/vcpkg/bootstrap-vcpkg.sh -disableMetrics

RUN git clone --depth 1 --branch "${MXL_REF}" https://github.com/dmf-mxl/mxl.git /src/mxl
WORKDIR /src/mxl
RUN cmake --preset Linux-Clang-Release \
      -DBUILD_DOCS=OFF -DBUILD_TESTS=OFF -DBUILD_TOOLS=ON -DBUILD_UTILS=OFF \
      -DCMAKE_INSTALL_PREFIX=/opt/mxl -DCMAKE_INSTALL_LIBDIR=lib \
 && cmake --build build/Linux-Clang-Release \
 && cmake --install build/Linux-Clang-Release \
 && install -m 0644 LICENSE.txt /opt/mxl/LICENSE.txt \
 && cp -r LICENSES /opt/mxl/LICENSES

FROM debian:trixie-20261027

ARG RUST_VERSION=1.96.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential clang libclang-dev pkg-config ca-certificates curl git \
      libcap2-bin \
 && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
      --default-toolchain "${RUST_VERSION}" --component clippy,rustfmt

COPY --from=libmxl /opt/mxl /opt/mxl
RUN echo /opt/mxl/lib > /etc/ld.so.conf.d/mxl.conf && ldconfig
ENV MXL_LIBRARY_PATH=/opt/mxl/lib/libmxl.so PATH=/opt/mxl/bin:$PATH
