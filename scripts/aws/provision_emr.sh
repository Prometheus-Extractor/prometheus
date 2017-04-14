aws emr create-cluster \
  --name "Prometheus Cluster" \
  --release-label emr-4.9.1 \
  --applications Name=Spark \
  --use-default-roles \
  --ec2-attributes KeyName="prometheus-keypair",SubnetId=subnet-a5be57c2 \
  --configurations https://s3-eu-west-1.amazonaws.com/sony-prometheus-data/emrconfig.json \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r4.2xlarge InstanceGroupType=CORE,InstanceCount=4,InstanceType=r4.2xlarge
