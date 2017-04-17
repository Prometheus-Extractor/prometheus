aws emr create-cluster \
  --name "Prometheus Cluster" \
  --release-label emr-4.9.1 \
  --applications Name=Spark \
  --use-default-roles \
  --ec2-attributes KeyName="prometheus-keypair",SubnetId=subnet-a5be57c2 \
  --configurations https://s3-eu-west-1.amazonaws.com/sony-prometheus-data/emrconfig.json \
  --instance-fleets InstanceFleetType=MASTER,TargetOnDemandCapacity=1,InstanceTypeConfigs=['{InstanceType=r4.2xlarge}'] InstanceFleetType=CORE,TargetSpotCapacity=8,InstanceTypeConfigs=['{InstanceType=r4.2xlarge,BidPrice=0.5,WeightedCapacity=1}','{InstanceType=r4.8xlarge,BidPrice=0.5,WeightedCapacity=1}','{InstanceType=r4.4xlarge,BidPrice=0.5,WeightedCapacity=1}','{InstanceType=m4.4xlarge,BidPrice=0.5,WeightedCapacity=1}','{InstanceType=m4.10xlarge,BidPrice=0.5,WeightedCapacity=1}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=5,TimeoutAction=TERMINATE_CLUSTER}'}
