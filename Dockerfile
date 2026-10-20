FROM ghcr.io/denoland/celld:latest

WORKDIR /app

ENTRYPOINT ["sh", "-c", "celld --bucket s3://$BUCKET_NAME --endpoint $AWS_ENDPOINT_URL_S3 --listen 0.0.0.0:8080 --internal-listen 127.0.0.1:9876"]

