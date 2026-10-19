FROM debian:bookworm-slim
# Download the celld binary
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://celld.dev/install.sh | sh
# Run the daemon
CMD ["celld", "start"]
