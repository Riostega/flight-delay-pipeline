"""Kill switch: terminate the pipeline EC2 instance and stop all EC2 billing.

Terminating destroys the instance and its EBS volume. That is safe here because
nothing on the box is a source of truth — raw data lives in S3, and
infra/provision_ec2.py rebuilds the host from scratch in a few minutes.

    python3 infra/terminate_ec2.py          # show what would be terminated
    python3 infra/terminate_ec2.py --yes    # actually terminate

Requires .env.admin. If that has already been deleted, terminate from the
console instead: EC2 > Instances > select > Instance state > Terminate.
"""

import sys

from pathlib import Path

import boto3
from dotenv import dotenv_values

NAME = "flight-pipeline"

# Resolved from this file's location, not the working directory. These scripts
# are run from wherever you happen to be — the teardown especially, which you
# reach for in an emergency — and a relative path made them report that
# credentials were missing when they were merely elsewhere.
REPO_ROOT = Path(__file__).resolve().parent.parent
env = dotenv_values(REPO_ROOT / ".env")
admin = dotenv_values(REPO_ROOT / ".env.admin")
region = (env.get("AWS_REGION") or "us-east-2").strip()

key = (admin.get("AWS_ADMIN_ACCESS_KEY_ID") or "").strip()
secret = (admin.get("AWS_ADMIN_SECRET_ACCESS_KEY") or "").strip()
if not key or not secret:
    sys.exit(
        "No admin credentials in .env.admin.\n"
        "Terminate from the console: EC2 > Instances > select > Instance state > Terminate."
    )

ec2 = boto3.client(
    "ec2",
    aws_access_key_id=key,
    aws_secret_access_key=secret,
    region_name=region,
)

# The region decides where this looks. A wrong AWS_REGION in .env would find
# nothing while the real host keeps running (and billing) somewhere else, so say
# which region was searched and where that came from.
source = "from .env" if env.get("AWS_REGION") else "default; AWS_REGION is not set in .env"
print(f"region {region} ({source})")

res = ec2.describe_instances(Filters=[
    {"Name": "tag:Name", "Values": [NAME]},
    {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
])["Reservations"]

instances = [i for r in res for i in r["Instances"]]
if not instances:
    print(
        f"No instances tagged Name={NAME} (pending/running/stopping/stopped) in {region}.\n"
        "If the host should exist, check AWS_REGION in .env or look in the EC2 console."
    )
    sys.exit(0)

for i in instances:
    print(f"  {i['InstanceId']}  {i['InstanceType']}  {i['State']['Name']}  {i.get('PublicIpAddress', '-')}")

if "--yes" not in sys.argv:
    print("\nDry run. Re-run with --yes to terminate.")
    sys.exit(0)

ids = [i["InstanceId"] for i in instances]
ec2.terminate_instances(InstanceIds=ids)
print(f"\nTerminating {', '.join(ids)} ...")
ec2.get_waiter("instance_terminated").wait(InstanceIds=ids)
print(f"Terminated. This instance's EC2 billing in {region} has stopped.")
print("S3 and Snowflake are untouched. Rebuild with: python3 infra/provision_ec2.py --launch")
