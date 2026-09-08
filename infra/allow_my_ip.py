#!/usr/bin/env python3
"""Point the host's SSH rule at whatever network you are on right now.

    python3 infra/allow_my_ip.py

A residential or public IP changes every time you move networks — home, a cafe,
a hotel, a conference. The security group allows exactly one address, so SSH
starts timing out (a timeout, not a refusal: the packets are dropped, the host
is fine). This swaps the rule to your current address and removes the previous
one, because an address you have left still holds SSH to the instance for
whoever the network hands it to next.

Requires .env.admin. If that has been deleted, do the same two clicks in the
EC2 console: Security Groups -> flight-pipeline-sg -> Edit inbound rules.

You usually do NOT need this. The pipeline runs unattended and reports failures
to Slack and to the external heartbeat; freshness can be confirmed straight from
Snowflake. SSH is only for looking around once something has already told you to.
"""

import sys
import urllib.request

import boto3
from dotenv import dotenv_values
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SG_NAME = "flight-pipeline-sg"

admin = dotenv_values(REPO_ROOT / ".env.admin")
env = dotenv_values(REPO_ROOT / ".env")

key = (admin.get("AWS_ADMIN_ACCESS_KEY_ID") or "").strip()
secret = (admin.get("AWS_ADMIN_SECRET_ACCESS_KEY") or "").strip()
if not key or not secret:
    # Explicit rather than falling through to boto3's ambient credential chain,
    # which would silently act as whichever identity happens to be configured.
    sys.exit("No admin credentials in .env.admin — edit the rule in the EC2 console instead.")

ec2 = boto3.client(
    "ec2",
    region_name=(env.get("AWS_REGION") or "us-east-2").strip(),
    aws_access_key_id=key,
    aws_secret_access_key=secret,
)

ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=15).read().decode().strip()
cidr = f"{ip}/32"
print(f"current address: {cidr}")

groups = ec2.describe_security_groups(
    Filters=[{"Name": "group-name", "Values": [SG_NAME]}]
)["SecurityGroups"]
if not groups:
    sys.exit(f"security group {SG_NAME} not found")
sg = groups[0]
sgid = sg["GroupId"]

existing = [
    r["CidrIp"]
    for p in sg["IpPermissions"]
    if p.get("FromPort") == 22
    for r in p.get("IpRanges", [])
]

if cidr in existing:
    print("already allowed — nothing to do")
else:
    ec2.authorize_security_group_ingress(
        GroupId=sgid,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
            "IpRanges": [{"CidrIp": cidr, "Description": "SSH from current host"}],
        }],
    )
    print(f"allowed {cidr}")

for stale in [c for c in existing if c != cidr]:
    ec2.revoke_security_group_ingress(
        GroupId=sgid,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
            "IpRanges": [{"CidrIp": stale}],
        }],
    )
    print(f"revoked {stale}")

print("\nSSH should work now.")
