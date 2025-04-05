import os
import hashlib
import tempfile
import pytest
import pandas as pd
from moto.s3 import mock_s3
import boto3
from s3_divvy import core

@pytest.fixture
def dummy_s3_bucket():
    with mock_s3():
        bucket = "test-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="a.csv", Body="data-a", ContentLength=6)
        s3.put_object(Bucket=bucket, Key="b.csv", Body="data-b", ContentLength=6)
        yield bucket


def test_list_s3_files(monkeypatch, dummy_s3_bucket):
    monkeypatch.setattr(core, "S3_BUCKET", dummy_s3_bucket)
    df = core.list_s3_files()
    assert isinstance(df, pd.DataFrame)
    assert set(df["file_name"]) == {"a.csv", "b.csv"}


def test_save_file_hash(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")

    core.HASH_DIR = str(tmp_path)
    core.save_file_hash(str(file_path))

    hash_path = tmp_path / "test.txt.sha256"
    assert hash_path.exists()

    # Check hash content
    expected = hashlib.sha256(b"hello world").hexdigest()
    actual = hash_path.read_text().strip()
    assert actual == expected
