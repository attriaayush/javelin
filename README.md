# Javelin

## Steps

1.

```
export AWS_SECRET_ACCESS_KEY=<secret_key>
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_DEFAULT_REGION=<region>
```

1.

```
cargo build --release
```

1.

```
<path-to-release-binary> --bucket-name <bucket_name> --dir-path <dir> --threads <count>
```
