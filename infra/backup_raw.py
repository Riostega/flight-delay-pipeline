"""Mirror the S3 raw zone to a local directory.

Everything downstream of S3 is reproducible: the warehouse rebuilds from these
files, the models live in git, the host rebuilds from bootstrap.sh. S3 itself is
the one layer with nothing upstream of it, so it is the one layer worth copying.

Bucket versioning already protects against a deletion. This protects against
losing the account: a suspended AWS account, a closed card, a credential leak
that forces a teardown. Different failure, different mitigation.

The whole raw zone is about 13 MB, so this is a full copy rather than anything
clever, and it skips files it already has — safe to re-run as often as you like.

    python3 infra/backup_raw.py                    # ~/flight-pipeline-backup
    python3 infra/backup_raw.py /Volumes/USB/raw   # somewhere else

Keep one copy offsite. Three copies, two media, one elsewhere is the old rule
and it still holds: S3, this machine, and whatever cloud drive you already pay
for.
"""

import sys
from pathlib import Path

import boto3
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent.parent
BUCKET = "flight-delay-pipeline-josh"
PREFIXES = ("raw/flights/", "raw/weather/")


def main():
    destination = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else Path.home() / "flight-pipeline-backup"

    env = dotenv_values(REPO_ROOT / ".env")
    if not env.get("AWS_ACCESS_KEY_ID"):
        sys.exit(f"No AWS credentials in {REPO_ROOT / '.env'}")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env.get("AWS_REGION", "us-east-2"),
    )

    print(f"bucket:      s3://{BUCKET}")
    print(f"destination: {destination}\n")

    downloaded = skipped = failed = 0
    total_bytes = 0

    for prefix in PREFIXES:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                target = destination / key

                # Size match is enough here: these objects are written once and
                # never modified, so a file of the right length is the file.
                if target.exists() and target.stat().st_size == obj["Size"]:
                    skipped += 1
                    continue

                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    s3.download_file(BUCKET, key, str(target))
                    downloaded += 1
                    total_bytes += obj["Size"]
                except Exception as exc:  # keep going — one bad object is not a failed backup
                    failed += 1
                    print(f"  FAILED {key}: {exc}")

        print(f"  {prefix:15} done")

    print(f"\ndownloaded {downloaded} files ({total_bytes / 1e6:.1f} MB)")
    print(f"already had {skipped}")
    if failed:
        print(f"FAILED on {failed} — re-run to retry")
        sys.exit(1)

    local = sum(1 for p in destination.rglob("*") if p.is_file())
    print(f"backup now holds {local} files")


if __name__ == "__main__":
    main()
