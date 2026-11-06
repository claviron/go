FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates clang curl git python3 xz-utils binutils gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Official FreeBSD 14.4-RELEASE amd64 base, verified against its MANIFEST.
# This is a cross sysroot, not a FreeBSD runtime inside a Linux container.
RUN curl --fail --location --output /tmp/base.txz \
      https://download.freebsd.org/releases/amd64/amd64/14.4-RELEASE/base.txz \
    && echo '769f60a6eea2938ad6b7943cfbcc17dfaf7d1f7ba59a32b869a00349302df853  /tmp/base.txz' | sha256sum --check \
    && mkdir -p /opt/freebsd \
    && tar -xJf /tmp/base.txz -C /opt/freebsd ./lib ./usr/lib ./usr/include \
    && rm /tmp/base.txz

COPY cc /usr/local/bin/freebsd-cc
RUN chmod 755 /usr/local/bin/freebsd-cc
ENV PATH="/opt/rust/bin:/usr/local/bin:/usr/bin:/bin" \
    CARGO_HOME="/cache/cargo" \
    CARGO_TARGET_DIR="/cache/target" \
    CARGO_TARGET_X86_64_UNKNOWN_FREEBSD_LINKER="/usr/local/bin/freebsd-cc" \
    CC_x86_64_unknown_freebsd="/usr/local/bin/freebsd-cc" \
    CXX_x86_64_unknown_freebsd="/usr/local/bin/freebsd-cc"
WORKDIR /workspace/claviron-go
CMD ["python3", "scripts/release.py", "--prepare", "--target", "freebsd-x86_64"]
