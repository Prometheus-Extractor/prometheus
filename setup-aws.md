# AWS Setup

- Install AWS cli
- Configure using `aws configure`
- Create Key Pair and download certificate.
- Create a EMR cluster running Spark 1.6.3. See section below.
- Make sure the security group for the master allows SSH.
- SSH into master using `aws emr ssh --cluster-id <id> --key-pair-file <cert>`

Once all this is done, make sure the data is available in a S3 bucket.
Then configure the `run-aws.sh` script and use it to submit the job to the cluster.

## EMR Cluster config
This command below creates a 10 node Spark cluster. You may need to change things like key-pair name, vpc subnet id, security group and s3 buckets.
```
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Name=Spark --ec2-attributes '{"KeyName":"prometheus-keypair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-d5e9fea3","EmrManagedSlaveSecurityGroup":"sg-46b05e3f","EmrManagedMasterSecurityGroup":"sg-44b05e3d"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.8.4 --log-uri 's3n://sony-prometheus-logs/' --name 'Prometheus-Cluster' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"r3.xlarge","Name":"Master - 1"},{"InstanceCount":9,"InstanceGroupType":"CORE","InstanceType":"c3.xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region eu-west-1
```
