# Javelin

Upload directories to your S3 bucket concurrently. Uses `aws-sdk-s3`'s multipart upload implementation.

## Steps

```
export AWS_SECRET_ACCESS_KEY=<secret_key>
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_DEFAULT_REGION=<region>
```

```
cargo build --release
```

```
<path-to-release-binary> --bucket-name <bucket_name> --dir-path <dir> --threads <count>
```
