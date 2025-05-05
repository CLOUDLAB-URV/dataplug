# Credential Configuration Guide for CloudObject

This README explains how to provide credentials to **CloudObject**, Dataplug’s main class for accessing AWS S3 using temporary STS credentials.

---

## 1. Why use temporary credentials?

`CloudObject` internally uses `PickleableS3ClientProxy` to obtain temporary AWS STS credentials. This allows you to create instances of `CloudObject` (and send them to remote workers) without exposing long-lived keys.

---

## 2. Credential supply modes

When invoking factory methods such as `CloudObject.from_s3()` or `CloudObject.new_from_file()`, pass a configuration dictionary under the `s3_config` parameter. If you do not pass `s3_config`, the standard boto3 S3 client will be used, picking up credentials from environment variables or `~/.aws/credentials`.

| Mode                       | Key in `s3_config`          | Description                                                                                       |
|----------------------------|-----------------------------|---------------------------------------------------------------------------------------------------|
| **Environment variables**  | _none_                      | Uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`.              |
| **Explicit credentials**   | `credentials`               | `{"AccessKeyId": "AK...", "SecretAccessKey": "SK...", optional "SessionToken": "ST..."}`          |
| **Assume IAM role**        | `role_arn`                  | ARN of the role (`sts:AssumeRole`) with S3 permissions, e.g. `"arn:aws:iam::123456789012:role/S3FullAccess"` |
| **STS session token**      | _none_                      | If neither `credentials` nor `role_arn` is provided, `GetSessionToken` is called automatically.   |

Additional options:

- `region_name`: AWS region (e.g. `"us-west-2"`).
- `endpoint_url`: Custom endpoint (only for non-AWS S3-compatible services).
- `token_duration_seconds`: Token duration in seconds (default 86400 s = 24 h).
- `botocore_config_kwargs`: Additional parameters for `botocore.client.Config`.

---

## 3. Using environment variables

Simply export in your shell:

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_SESSION_TOKEN=YOUR_SESSION_TOKEN      # optional, if applicable
export AWS_REGION=us-west-2                    # optional
```

Then call `CloudObject.from_s3()` without `s3_config` and boto3 will pick up those variables:

```python
from dataplug import CloudObject
from dataplug.formats.generic.csv import CSV

co = CloudObject.from_s3(
    CSV,
    "s3://my-aws-bucket/data.csv"
)
```

---

## 4. AWS Example

Below is an AWS example demonstrating reading, preprocessing, and partitioning CSV data directly from an S3 bucket using different `s3_config` modes:

1. **Explicit AWS credentials**:

    ```python
    from dataplug import CloudObject
    from dataplug.formats.generic.csv import CSV, partition_num_chunks

    s3_config = {
        "credentials": {
            "AccessKeyId": "AKIA......",
            "SecretAccessKey": "SK...",
            "SessionToken": "ST..."
        },
        "region_name": "us-west-2"
    }
    co = CloudObject.from_s3(
        CSV,
        "s3://my-aws-bucket/cities.csv",
        s3_config=s3_config
    )
    ```

2. **Assume an IAM role**:

    ```python
    from dataplug import CloudObject
    from dataplug.formats.generic.csv import CSV, partition_num_chunks

    s3_config = {
        "role_arn": "arn:aws:iam::123456789012:role/S3FullAccess",
        "token_duration_seconds": 3600,
        "region_name": "us-west-2"
    }
    co = CloudObject.from_s3(
        CSV,
        "s3://my-aws-bucket/cities.csv",
        s3_config=s3_config
    )
    ```

3. **Process and partition**:

    ```python
    parallel_config = {"verbose": 10}
    co.preprocess(
        parallel_config=parallel_config,
        force=True
    )

    data_slices = co.partition(
        partition_num_chunks,
        num_chunks=25
    )

    for ds in data_slices:
        print(ds.get_as_pandas())
        print('---')
    ```
---

## 5. Upload a local file to S3

```python
from dataplug import CloudObject
from dataplug.formats.generic.csv import CSV

s3_config = {
    "role_arn": "arn:aws:iam::123456789012:role/S3FullAccess",
    "region_name": "us-west-2"
}

co_new = CloudObject.new_from_file(
    CSV,
    file_path="/local/path/file.csv",
    cloud_path="s3://my-aws-bucket/uploaded.csv",
    s3_config=s3_config,
    override=True
)
```
