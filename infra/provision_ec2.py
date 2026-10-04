"""Provision the EC2 host that runs the pipeline.

Creates, in order: an IAM role that can add files to the raw zone, an SSH key
pair, a security group allowing SSH from one address only, and a t3.micro
instance. Everything except the instance itself is free, so resources are
created first and the instance launch is gated behind --launch.

Credentials come from .env.admin (gitignored; keep it chmod 600). It is kept
after provisioning because allow_my_ip.py and terminate_ec2.py use it too.
Delete it, and use the EC2 console for those two jobs, once the project is done.

The instance gets no AWS keys on disk. It assumes the IAM role instead, and the
role can only list and add files under raw/ in the one bucket: it cannot read
or delete what is already there (and an overwrite keeps the old copy, because
the bucket is versioned). That does NOT mean there is nothing on the box worth
stealing. The host's .env (and the dbt profile made from it) holds the
Snowflake password, for a user with the trial account's ACCOUNTADMIN role, plus
the Slack webhook, the heartbeat URL and both API keys.
A compromised host is a compromised Snowflake account, which is why SSH is
locked to one address and the Airflow UI is never opened to the internet.

    python3 infra/provision_ec2.py            # create supporting resources
    python3 infra/provision_ec2.py --launch   # ...and launch the instance
"""

import json
import os
import sys
import time

from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import dotenv_values

NAME = "flight-pipeline"
ROLE_NAME = f"{NAME}-ec2-role"
PROFILE_NAME = f"{NAME}-ec2-profile"
SG_NAME = f"{NAME}-sg"
KEY_NAME = f"{NAME}-key"
KEY_PATH = os.path.expanduser(f"~/.ssh/{KEY_NAME}.pem")
INSTANCE_TYPE = "t3.micro"
AMI_FILTER = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
CANONICAL = "099720109477"

# Resolved from this file's location, not the working directory. These scripts
# are run from wherever you happen to be — the teardown especially, which you
# reach for in an emergency — and a relative path made them report that
# credentials were missing when they were merely elsewhere.
REPO_ROOT = Path(__file__).resolve().parent.parent
env = dotenv_values(REPO_ROOT / ".env")
admin = dotenv_values(REPO_ROOT / ".env.admin")
REGION = (env.get("AWS_REGION") or "us-east-2").strip()
BUCKET = (env.get("S3_BUCKET_NAME") or "").strip()

kw = dict(
    aws_access_key_id=(admin.get("AWS_ADMIN_ACCESS_KEY_ID") or "").strip(),
    aws_secret_access_key=(admin.get("AWS_ADMIN_SECRET_ACCESS_KEY") or "").strip(),
    region_name=REGION,
)
ec2 = boto3.client("ec2", **kw)
iam = boto3.client("iam", **kw)


def my_ip():
    """The address the security group will allow SSH from.

    Uses requests rather than shelling out to curl: one fewer external
    dependency, works the same on any platform, and carries a timeout so a
    hung lookup cannot stall provisioning.
    """
    return requests.get("https://checkip.amazonaws.com", timeout=10).text.strip()


def ensure_role():
    """Role the instance assumes: list and add files under raw/, nothing else.

    The host only ever calls list_objects_v2 (the quota check) and put_object
    (landing a file). No GetObject, and above all no DeleteObject: a compromised
    host can then add junk but cannot destroy the source of truth. Leaving out
    DeleteObjectVersion and PutBucketVersioning is what keeps bucket versioning a
    real backstop, because the host cannot remove old versions or switch it off.
    """
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # The quota check lists raw/flights/YYYY-MM. If this were scoped
                # wrongly the check would not fail loudly: it prints "budget
                # check unavailable; proceeding", so confirm the quota line
                # after changing it.
                "Sid": "ListRawForBudgetCheck",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET}",
                "Condition": {"StringLike": {"s3:prefix": ["raw/*"]}},
            },
            {
                "Sid": "LandRawFiles",
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{BUCKET}/raw/*",
            },
        ],
    }
    try:
        iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
        print(f"  created role {ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"  role {ROLE_NAME} already exists")

    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=f"{NAME}-s3", PolicyDocument=json.dumps(policy))
    print(f"  attached S3 policy: list and add under s3://{BUCKET}/raw/ (no read, no delete)")

    try:
        iam.create_instance_profile(InstanceProfileName=PROFILE_NAME)
        print(f"  created instance profile {PROFILE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"  instance profile {PROFILE_NAME} already exists")
    try:
        iam.add_role_to_instance_profile(InstanceProfileName=PROFILE_NAME, RoleName=ROLE_NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "LimitExceeded":
            raise
    return PROFILE_NAME


def ensure_key_pair():
    """Create the SSH key pair, reconciling local and AWS state.

    Checking only for the local file is not enough. If the private key were lost
    while the key pair still existed in AWS, recreating it would replace the key
    on record while a running instance kept the old public key — permanently
    locking you out of a box you can still see. And if the AWS key were deleted
    while the file remained, provisioning would skip creation and fail later
    with an unhelpful error from run_instances.
    """
    local = os.path.exists(KEY_PATH)
    # A filter query returns an empty list when the pair does not exist, so any
    # real error (no permission, throttling) still raises instead of being read
    # as "no key pair" — which would lead to the advice below to delete a key.
    remote = bool(ec2.describe_key_pairs(
        Filters=[{"Name": "key-name", "Values": [KEY_NAME]}]
    )["KeyPairs"])

    if local and remote:
        print(f"  key pair present locally and in AWS ({KEY_PATH})")
        return
    if local and not remote:
        # Key pairs are regional. A wrong AWS_REGION in .env looks exactly like a
        # missing key pair, and deleting the file then would throw away the only
        # key to a host that is running fine in the right region.
        users = [
            i["InstanceId"]
            for r in ec2.describe_instances(Filters=[
                {"Name": "key-name", "Values": [KEY_NAME]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
            ])["Reservations"]
            for i in r["Instances"]
        ]
        if users:
            sys.exit(
                f"Private key exists at {KEY_PATH}, and instance(s) {', '.join(users)} in {REGION} "
                f"still use '{KEY_NAME}', but the key pair itself is gone from AWS.\n"
                "Keep the local file: it is the only way into those instances."
            )
        sys.exit(
            f"Private key exists at {KEY_PATH} but no '{KEY_NAME}' key pair exists in AWS region {REGION}.\n"
            "Key pairs are regional, so first confirm AWS_REGION in .env is the region your "
            "instance runs in. Only if it is, move the local file aside (for example to "
            f"{KEY_PATH}.bak, rather than deleting it) and re-run to create a fresh pair."
        )
    if remote and not local:
        sys.exit(
            f"AWS has a '{KEY_NAME}' key pair but the private key is not at {KEY_PATH}.\n"
            "Refusing to replace it: a running instance still trusts the old public key, "
            "and recreating the pair would lock you out of it permanently.\n"
            "Recover the private key, or terminate the instance and delete the key pair first."
        )

    r = ec2.create_key_pair(KeyName=KEY_NAME, KeyType="ed25519")
    os.makedirs(os.path.dirname(KEY_PATH), exist_ok=True)
    # Created read-only for this user from the start, rather than written with
    # the default (world-readable) mode and tightened afterwards. O_EXCL is safe:
    # the checks above have already exited if the file exists.
    fd = os.open(KEY_PATH, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
    with os.fdopen(fd, "w") as f:
        f.write(r["KeyMaterial"])
    print(f"  created key pair, private key saved to {KEY_PATH} (chmod 400)")


def ensure_security_group():
    ip = my_ip()
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    try:
        sg = ec2.create_security_group(
            GroupName=SG_NAME, Description="Flight pipeline host: SSH from one address", VpcId=vpc
        )["GroupId"]
        print(f"  created security group {sg}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "InvalidGroup.Duplicate":
            raise
        sg = ec2.describe_security_groups(GroupNames=[SG_NAME])["SecurityGroups"][0]["GroupId"]
        print(f"  security group {sg} already exists")

    # Port 8080 is deliberately NOT opened. The Airflow UI is reached through an
    # SSH tunnel; an internet-facing Airflow is a genuinely bad idea.
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                "IpRanges": [{"CidrIp": f"{ip}/32", "Description": "SSH from provisioning host"}],
            }],
        )
        print(f"  allowed SSH from {ip}/32")
    except ClientError as e:
        if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise
        print(f"  SSH rule for {ip}/32 already present")

    # Revoke every other SSH rule. This used to only ever ADD, which meant the
    # group accumulated one permanent /32 for every network provisioning was
    # ever run from. On a dynamic residential address that is a real exposure:
    # when the ISP rotates the IP, SSH breaks, the natural fix is to re-run this
    # script — and the address you just stopped using keeps SSH to the host for
    # whoever the ISP hands it to next. The group's own description says "SSH
    # from one address", so make that true rather than aspirational.
    current = ec2.describe_security_groups(GroupIds=[sg])["SecurityGroups"][0]
    for perm in current["IpPermissions"]:
        # Only plain tcp/22 rules, the kind this script creates. Revoking needs
        # an exact match, so a 22-80 range here would crash the revoke; edit
        # anything unusual in the console (allow_my_ip.py reports such rules).
        if (perm.get("IpProtocol"), perm.get("FromPort"), perm.get("ToPort")) != ("tcp", 22, 22):
            continue
        stale = [r for r in perm.get("IpRanges", []) if r["CidrIp"] != f"{ip}/32"]
        for rng in stale:
            ec2.revoke_security_group_ingress(
                GroupId=sg,
                IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                    "IpRanges": [{"CidrIp": rng["CidrIp"]}],
                }],
            )
            print(f"  revoked stale SSH rule {rng['CidrIp']}")
    return sg


def latest_ami():
    imgs = ec2.describe_images(
        Owners=[CANONICAL],
        Filters=[{"Name": "name", "Values": [AMI_FILTER]}, {"Name": "state", "Values": ["available"]}],
    )["Images"]
    img = sorted(imgs, key=lambda x: x["CreationDate"])[-1]
    print(f"  AMI {img['ImageId']} ({img['Name']})")
    return img["ImageId"]


USER_DATA = """#!/bin/bash
set -eux
# 4GB swap: Airflow idles near 1GB and this box has 1GB of RAM.
fallocate -l 4G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
sysctl vm.swappiness=10
echo 'vm.swappiness=10' >> /etc/sysctl.conf

apt-get update
apt-get install -y python3.12 python3.12-venv python3-pip git rsync
touch /home/ubuntu/.provisioned
"""


def launch(profile, sg, ami):
    existing = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [NAME]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])["Reservations"]
    if existing:
        i = existing[0]["Instances"][0]
        print(f"  instance already exists: {i['InstanceId']} ({i['State']['Name']})")
        return i["InstanceId"]

    r = ec2.run_instances(
        ImageId=ami, InstanceType=INSTANCE_TYPE, KeyName=KEY_NAME,
        SecurityGroupIds=[sg], MinCount=1, MaxCount=1,
        IamInstanceProfile={"Name": profile},
        UserData=USER_DATA,
        BlockDeviceMappings=[{"DeviceName": "/dev/sda1",
                              "Ebs": {"VolumeSize": 20, "VolumeType": "gp3", "DeleteOnTermination": True}}],
        TagSpecifications=[{"ResourceType": "instance",
                            "Tags": [{"Key": "Name", "Value": NAME},
                                     {"Key": "Project", "Value": "flight-delay-pipeline"}]}],
    )
    iid = r["Instances"][0]["InstanceId"]
    print(f"  launched {iid} ({INSTANCE_TYPE}), waiting for it to run...")
    ec2.get_waiter("instance_running").wait(InstanceIds=[iid])
    ip = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0].get("PublicIpAddress")
    print(f"  running at {ip}")
    print(f"\n  ssh -i {KEY_PATH} ubuntu@{ip}")
    return iid


if __name__ == "__main__":
    # Checked explicitly, as allow_my_ip.py does. Otherwise empty credentials
    # reach boto3 and the first IAM call fails with "InvalidClientTokenId",
    # which reads like a bad key rather than a missing file.
    if not kw["aws_access_key_id"] or not kw["aws_secret_access_key"]:
        sys.exit(
            f"No admin credentials in {REPO_ROOT / '.env.admin'}.\n"
            "Create it with AWS_ADMIN_ACCESS_KEY_ID / AWS_ADMIN_SECRET_ACCESS_KEY from an "
            "IAM admin user (chmod 600), then re-run."
        )
    if not BUCKET:
        sys.exit("S3_BUCKET_NAME missing from .env")
    print(f"region {REGION}, bucket {BUCKET}\n")
    print("IAM role")
    profile = ensure_role()
    print("\nSSH key pair")
    ensure_key_pair()
    print("\nSecurity group")
    sg = ensure_security_group()
    print("\nAMI")
    ami = latest_ami()

    if "--launch" in sys.argv:
        print("\nInstance")
        # IAM propagation is eventually consistent; a fresh instance profile is
        # not always usable immediately.
        time.sleep(10)
        launch(profile, sg, ami)
    else:
        print("\nSupporting resources ready (all free). Re-run with --launch to start the instance.")
