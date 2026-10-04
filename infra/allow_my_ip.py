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

# Only plain IPv4 tcp/22 rules, the kind this script and provision_ec2.py
# create. Revoking needs an exact match, so anything else (a 22-80 range, an
# IPv6 or "All traffic" rule) is left alone here and reported at the end.
existing = [
    r["CidrIp"]
    for p in sg["IpPermissions"]
    if (p.get("IpProtocol"), p.get("FromPort"), p.get("ToPort")) == ("tcp", 22, 22)
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

# The group is meant to allow exactly one thing: SSH from this address. Report
# anything else, such as an IPv6 or "All traffic" rule added in the console
# while debugging, rather than letting "SSH from one address" quietly stop being
# true. Reported, not deleted: this script did not create those rules, and
# someone may have added one on purpose.
sg = ec2.describe_security_groups(GroupIds=[sgid])["SecurityGroups"][0]
unexpected = []
for p in sg["IpPermissions"]:
    proto, lo, hi = p.get("IpProtocol"), p.get("FromPort"), p.get("ToPort")
    ports = "all traffic" if proto == "-1" else f"{proto} {lo}" + ("" if hi == lo else f"-{hi}")
    sources = (
        [r["CidrIp"] for r in p.get("IpRanges", [])]
        + [r["CidrIpv6"] for r in p.get("Ipv6Ranges", [])]
        + [r["PrefixListId"] for r in p.get("PrefixListIds", [])]
        + [g["GroupId"] for g in p.get("UserIdGroupPairs", [])]
    )
    for src in sources:
        if not (proto == "tcp" and lo == hi == 22 and src == cidr):
            unexpected.append(f"{ports} from {src}")

if unexpected:
    print(f"\nWARNING: {SG_NAME} allows more than SSH from {cidr}:")
    for rule in unexpected:
        print(f"  {rule}")
    print("Remove these in the EC2 console unless they are there on purpose.")
    sys.exit(1)
