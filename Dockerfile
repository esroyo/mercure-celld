FROM ghcr.io/denoland/celld:latest

# 1. Install Deno & system tools
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://deno.land/x/install/install.sh | sh
ENV PATH="/root/.deno/bin:$PATH"

# 2. Extract native esbuild binary directly from @esbuild/linux-x64
RUN curl -fsSL https://registry.npmjs.org/@esbuild/linux-x64/-/linux-x64-0.28.2.tgz | tar -xz -C /tmp && \
    mv /tmp/package/bin/esbuild /usr/local/bin/esbuild && \
    chmod +x /usr/local/bin/esbuild && \
    rm -rf /tmp/package

ENV CELLD_ESBUILD="/usr/local/bin/esbuild"

WORKDIR /app

# 3. Cache dependencies
COPY deno.json* deno.lock* ./
RUN deno install --node-modules-dir=true

# 4. Copy source files
COPY . .

CMD ["dev", "--host", "0.0.0.0"]
