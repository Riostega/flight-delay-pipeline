"""Provision the EC2 host that runs the pipeline.

Creates, in order: an IAM role granting S3 access to the raw zone, an SSH key
pair, a security group allowing SSH from one address only, and a t3.micro
instance. Everything except the instance itself is free, so resources are
created first and the instance launch is gated behind --launch.

Credentials come from .env.admin (gitignored, deleted once provisioning is
done). The instance itself gets no credentials on disk — it assumes the IAM
role instead, so there is nothing on the box worth stealing.

    python3 infra/provision_ec2.py            # create supporting resources
    python3 infra/provision_ec2.py --launch   # ...and launch the instance
"""

import json
import os
import sys
import time

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

env = dotenv_values(".env")
admin = dotenv_values(".env.admin")
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
    """Role the instance assumes, scoped to this project's bucket only."""
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
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
        }],
    }
    try:
        iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
        print(f"  created role {ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"  role {ROLE_NAME} already exists")

    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=f"{NAME}-s3", PolicyDocument=json.dumps(policy))
    print(f"  attached S3 policy scoped to {BUCKET}")

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
    if os.path.exists(KEY_PATH):
        print(f"  key already saved at {KEY_PATH}")
        return
    try:
        ec2.delete_key_pair(KeyName=KEY_NAME)
    except ClientError:
        pass
    r = ec2.create_key_pair(KeyName=KEY_NAME, KeyType="ed25519")
    os.makedirs(os.path.dirname(KEY_PATH), exist_ok=True)
    with open(KEY_PATH, "w") as f:
        f.write(r["KeyMaterial"])
    os.chmod(KEY_PATH, 0o400)
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
        print(f"  allowed SSH from {ip}/32 only")
    except ClientError as e:
        if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise
        print(f"  SSH rule for {ip}/32 already present")
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
