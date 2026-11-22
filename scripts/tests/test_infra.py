"""Tests for scripts/check_infra.py and scripts/upload_extracts.py.

AWS is mocked in-process with moto's mock_aws: no Docker, network or account.
"""

import json
from pathlib import Path

import boto3
import pytest
from check_infra import check, least_privilege_problems
from load_raw import TABLES
from moto import mock_aws
from upload_extracts import object_key, upload_all

REGION = "ca-central-1"
BUCKETS = ["ecom-dev-raw", "ecom-dev-lakehouse"]
LOGS = "ecom-dev-access-logs"


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch):
    # In-process mock: no endpoint URL, fake credentials
    monkeypatch.setenv("AWS_ENDPOINT_URL", "")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with mock_aws():
        yield boto3.client("s3", region_name=REGION), boto3.client("iam")


def policy(statements: list[dict]) -> str:
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


GOOD = [
    {
        "Sid": "List",
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": [f"arn:aws:s3:::{b}" for b in BUCKETS],
    },
    {
        "Sid": "Raw",
        "Effect": "Allow",
        "Action": ["s3:PutObject", "s3:GetObject"],
        "Resource": ["arn:aws:s3:::ecom-dev-raw/*"],
    },
]


def provision(s3, iam, statements=GOOD) -> None:
    """What infra/terraform creates, done with boto3."""
    key = boto3.client("kms", region_name=REGION).create_key()["KeyMetadata"]["Arn"]
    for bucket in [*BUCKETS, LOGS]:
        s3.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        s3.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": key,
                        }
                    }
                ]
            },
        )
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration=dict.fromkeys(
                [
                    "BlockPublicAcls",
                    "IgnorePublicAcls",
                    "BlockPublicPolicy",
                    "RestrictPublicBuckets",
                ],
                True,
            ),
        )
    s3.put_bucket_acl(Bucket=LOGS, ACL="log-delivery-write")
    for bucket in BUCKETS:
        s3.put_bucket_logging(
            Bucket=bucket,
            BucketLoggingStatus={
                "LoggingEnabled": {"TargetBucket": LOGS, "TargetPrefix": f"{bucket}/"}
            },
        )
    s3.put_bucket_lifecycle_configuration(
        Bucket=BUCKETS[0],
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire",
                    "Status": "Enabled",
                    "Filter": {},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 90},
                }
            ]
        },
    )
    iam.create_role(RoleName="ecom-dev-pipeline", AssumeRolePolicyDocument=policy([]))
    arn = iam.create_policy(
        PolicyName="ecom-dev-pipeline-s3", PolicyDocument=policy(statements)
    )["Policy"]["Arn"]
    iam.attach_role_policy(RoleName="ecom-dev-pipeline", PolicyArn=arn)


def test_a_correct_setup_passes(aws) -> None:
    provision(*aws)
    assert check(REGION, "ecom", "dev") == []


def test_unsafe_buckets_are_reported(aws) -> None:
    s3, iam = aws
    provision(s3, iam)
    s3.put_bucket_versioning(
        Bucket=BUCKETS[0], VersioningConfiguration={"Status": "Suspended"}
    )
    s3.delete_public_access_block(Bucket=BUCKETS[1])
    s3.put_public_access_block(
        Bucket=BUCKETS[1],
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": True,
        },
    )
    problems = check(REGION, "ecom", "dev")
    assert "ecom-dev-raw: versioning is not enabled" in problems
    assert any("lakehouse: public access not fully blocked" in p for p in problems)


def test_missing_role_is_reported(aws) -> None:
    s3, _ = aws
    for bucket in BUCKETS:
        s3.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    problems = check(REGION, "ecom", "dev")
    assert "role ecom-dev-pipeline not found" in problems
    # Settings never configured are reported, not a crash
    assert "ecom-dev-raw: no default encryption" in problems
    assert "ecom-dev-raw: no public access block" in problems
    assert "ecom-dev-raw: no rule expiring old object versions" in problems


def test_least_privilege_rules() -> None:
    assert least_privilege_problems(GOOD, BUCKETS) == []
    wildcard = [{"Sid": "All", "Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
    found = least_privilege_problems(wildcard, BUCKETS)
    assert any("wildcard action" in p for p in found)
    assert any("resources outside the buckets" in p for p in found)
    destroy = [
        {
            "Sid": "Oops",
            "Effect": "Allow",
            "Action": ["s3:DeleteBucket"],
            "Resource": ["arn:aws:s3:::ecom-dev-raw"],
        }
    ]
    assert any("destructive" in p for p in least_privilege_problems(destroy, BUCKETS))
    # A Deny statement is never a privilege problem
    deny = [{"Effect": "Deny", "Action": "s3:*", "Resource": "*"}]
    assert least_privilege_problems(deny, BUCKETS) == []


def test_upload_is_content_addressed_and_idempotent(aws, tmp_path: Path) -> None:
    s3, iam = aws
    provision(s3, iam)
    for table in TABLES:
        (tmp_path / f"{table}.csv").write_text(f"id\n{table}\n")
    first = upload_all(tmp_path, BUCKETS[0], s3)
    again = upload_all(tmp_path, BUCKETS[0], s3)
    assert [r.uploaded for r in first] == [True] * len(TABLES)
    assert [r.uploaded for r in again] == [False] * len(TABLES)
    head = s3.head_object(Bucket=BUCKETS[0], Key=first[0].key)
    assert head["Metadata"]["table"] == TABLES[0]
    assert first[0].key == object_key(TABLES[0], head["Metadata"]["sha256"])

    # A changed file is a new object; the old one is kept
    (tmp_path / "orders.csv").write_text("id\nchanged\n")
    third = upload_all(tmp_path, BUCKETS[0], s3)
    assert [r.table for r in third if r.uploaded] == ["orders"]
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKETS[0])["Contents"]]
    assert len(keys) == len(TABLES) + 1


def test_upload_needs_every_extract(aws, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="missing extracts"):
        upload_all(tmp_path, BUCKETS[0], aws[0])


def test_aes_encryption_and_missing_logs_are_reported(aws) -> None:
    s3, iam = aws
    provision(s3, iam)
    s3.put_bucket_encryption(
        Bucket=BUCKETS[1],
        ServerSideEncryptionConfiguration={
            "Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        },
    )
    s3.put_bucket_logging(Bucket=BUCKETS[0], BucketLoggingStatus={})
    problems = check(REGION, "ecom", "dev")
    assert "ecom-dev-lakehouse: not encrypted with a KMS key (['AES256'])" in problems
    assert "ecom-dev-raw: access logs not sent to ecom-dev-access-logs" in problems


def test_kms_permissions_only_on_keys() -> None:
    key = "arn:aws:kms:ca-central-1:123456789012:key/abc"
    use = [
        {"Sid": "Key", "Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": [key]}
    ]
    assert least_privilege_problems(use, BUCKETS) == []
    anywhere = [
        {"Sid": "Key", "Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": "*"}
    ]
    assert any("non-key" in p for p in least_privilege_problems(anywhere, BUCKETS))
    delete = [
        {
            "Sid": "Key",
            "Effect": "Allow",
            "Action": ["kms:ScheduleKeyDeletion"],
            "Resource": [key],
        }
    ]
    assert any("destructive" in p for p in least_privilege_problems(delete, BUCKETS))
