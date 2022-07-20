import re

S3_PATH_REGEX = re.compile(r'^\w+://.+/.+$')


def split_s3_path(path):
    if not S3_PATH_REGEX.fullmatch(path):
        raise ValueError(f'Path must satisfy regex {S3_PATH_REGEX}')

    bucket, key = path.replace('s3://', '').split('/', 1)
    return bucket, key
