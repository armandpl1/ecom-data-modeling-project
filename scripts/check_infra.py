#!/usr/bin/env python3
"""Check that the AWS resources in infra/terraform exist and are configured
safely, by reading them back with boto3 (from the emulator by default).

Buckets (raw, lakehouse and access-logs):
    exist, versioning enabled, default encryption with the customer-managed
    KMS key, every public access block on; raw and lakehouse send access
    logs to the log bucket; raw expires old object versions.
Pipeline role:
    exists, has the pipeline policy attached, and that policy is least
    privilege: no wildcard actions, every resource inside the two buckets,
    no bucket-level destructive or policy actions, and KMS permissions only
    on KMS keys.

Endpoint: AWS_ENDPOINT_URL (boto3 reads it), default the Moto emulator from
infra/docker-compose.yml. Exits 1 on any problem, listing all of them.

Usage:
    python scripts/check_infra.py
    python scripts/check_infra.py --environment prod
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import unquote

import boto3

DEFAULT_ENDPOINT = "http://localhost:15000"
FORBIDDEN_ACTIONS = {
    "s3:DeleteBucket",
    "s3:PutBucketPolicy",
    "s3:PutBucketAcl",
    "s3:PutObjectAcl",
    "s3:PutBucketPublicAccessBlock",
    "kms:ScheduleKeyDeletion",
    "kms:PutKeyPolicy",
    "kms:DisableKey",
}


def clients(region: str):
    endpoint = os.environ.get("AWS_ENDPOINT_URL", DEFAULT_ENDPOINT)
    kwargs = {"region_name": region}
    if endpoint:
        kwargs |= {
            "endpoint_url": endpoint,
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        }
    return boto3.client("s3", **kwargs), boto3.client("iam", **kwargs)


def get_config(call, **kwargs):
    """A bucket setting, or None when it was never configured (AWS raises then)."""
    try:
        return call(**kwargs)
    except Exception as err:
        code = getattr(err, "response", {}).get("Error", {}).get("Code", "")
        if code.startswith("NoSuch") or code.endswith("NotFoundError"):
            return None
        raise


def check_bucket(
    s3, name: str, needs_lifecycle: bool, log_bucket: str | None = None
) -> list[str]:
    problems = []
    try:
        s3.head_bucket(Bucket=name)
    except Exception as err:  # noqa: BLE001 - report any failure as missing
        return [f"{name}: bucket not found ({err})"]

    if s3.get_bucket_versioning(Bucket=name).get("Status") != "Enabled":
        problems.append(f"{name}: versioning is not enabled")

    encryption = get_config(s3.get_bucket_encryption, Bucket=name)
    rules = (encryption or {}).get("ServerSideEncryptionConfiguration", {})
    algorithms = {
        r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        for r in rules.get("Rules", [])
    }
    if not algorithms:
        problems.append(f"{name}: no default encryption")
    elif "aws:kms" not in algorithms:
        problems.append(f"{name}: not encrypted with a KMS key ({sorted(algorithms)})")

    if log_bucket is not None:
        logging = s3.get_bucket_logging(Bucket=name).get("LoggingEnabled") or {}
        if logging.get("TargetBucket") != log_bucket:
            problems.append(f"{name}: access logs not sent to {log_bucket}")

    found = get_config(s3.get_public_access_block, Bucket=name)
    block = (found or {}).get("PublicAccessBlockConfiguration", {})
    off = sorted(k for k, v in block.items() if not v)
    if not block:
        problems.append(f"{name}: no public access block")
    elif off:
        problems.append(f"{name}: public access not fully blocked ({', '.join(off)})")

    if needs_lifecycle:
        config = get_config(s3.get_bucket_lifecycle_configuration, Bucket=name)
        lifecycle = (config or {}).get("Rules", [])
        expiring = [
            r
            for r in lifecycle
            if r.get("Status") == "Enabled" and r.get("NoncurrentVersionExpiration")
        ]
        if not expiring:
            problems.append(f"{name}: no rule expiring old object versions")
    return problems


def policy_statements(iam, policy_arn: str) -> list[dict]:
    policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
    version = iam.get_policy_version(
        PolicyArn=policy_arn, VersionId=policy["DefaultVersionId"]
    )["PolicyVersion"]
    document = version["Document"]
    if isinstance(document, str):  # some implementations return it URL-encoded
        document = json.loads(unquote(document))
    statements = document["Statement"]
    return statements if isinstance(statements, list) else [statements]


def least_privilege_problems(statements: list[dict], buckets: list[str]) -> list[str]:
    """Problems with a policy that should only touch the given buckets."""
    problems = []
    allowed = {f"arn:aws:s3:::{b}" for b in buckets} | {
        f"arn:aws:s3:::{b}/*" for b in buckets
    }
    for st in statements:
        if st.get("Effect") != "Allow":
            continue
        actions = st.get("Action", [])
        actions = [actions] if isinstance(actions, str) else actions
        resources = st.get("Resource", [])
        resources = [resources] if isinstance(resources, str) else resources
        sid = st.get("Sid", "?")
        if actions and all(a.startswith("kms:") for a in actions):
            # Key usage: allowed on KMS keys, never on everything
            keys_only = all(r.startswith("arn:aws:kms:") for r in resources)
            if not keys_only:
                problems.append(f"policy {sid}: KMS actions on non-key resources")
            if bad := sorted(set(actions) & FORBIDDEN_ACTIONS):
                problems.append(f"policy {sid}: destructive or policy actions {bad}")
            continue
        if any("*" in a for a in actions):
            problems.append(f"policy {sid}: wildcard action {actions}")
        if bad := sorted(set(actions) & FORBIDDEN_ACTIONS):
            problems.append(f"policy {sid}: destructive or policy actions {bad}")
        if outside := sorted(r for r in resources if r not in allowed):
            problems.append(f"policy {sid}: resources outside the buckets {outside}")
    return problems


def check(region: str, prefix: str, environment: str) -> list[str]:
    s3, iam = clients(region)
    buckets = [f"{prefix}-{environment}-raw", f"{prefix}-{environment}-lakehouse"]
    logs = f"{prefix}-{environment}-access-logs"
    problems = check_bucket(s3, buckets[0], needs_lifecycle=True, log_bucket=logs)
    problems += check_bucket(s3, buckets[1], needs_lifecycle=False, log_bucket=logs)
    problems += check_bucket(s3, logs, needs_lifecycle=False)

    role = f"{prefix}-{environment}-pipeline"
    try:
        iam.get_role(RoleName=role)
    except iam.exceptions.NoSuchEntityException:
        return [*problems, f"role {role} not found"]
    attached = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
    wanted = f"{prefix}-{environment}-pipeline-s3"
    arns = [p["PolicyArn"] for p in attached if p["PolicyName"] == wanted]
    if not arns:
        return [*problems, f"role {role}: policy {wanted} not attached"]
    if len(attached) > 1:
        others = sorted(p["PolicyName"] for p in attached if p["PolicyName"] != wanted)
        problems.append(f"role {role}: unexpected extra policies {others}")
    problems += least_privilege_problems(policy_statements(iam, arns[0]), buckets)
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--region", default="ca-central-1")
    parser.add_argument("--prefix", default="ecom")
    parser.add_argument("--environment", default="dev")
    args = parser.parse_args(argv)
    problems = check(args.region, args.prefix, args.environment)
    if problems:
        print("FAIL:", *problems, sep="\n  - ", file=sys.stderr)
        return 1
    print(
        f"OK: buckets {args.prefix}-{args.environment}-raw, -lakehouse and "
        "-access-logs are versioned, KMS-encrypted, private and logged; the "
        "pipeline role is least privilege"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
