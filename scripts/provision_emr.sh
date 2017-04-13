aws emr create-cluster --name "Prometheus Cluster" --release-label emr-4.9.1 \
  --applications Name=Spark --ec2-attributes KeyName=prometheus-keypair \
  --instance-type r4.2xlarge --instance-count 5 --use-default-roles \
  --ec2-attributes SubnetId=subnet-a5be57c2
