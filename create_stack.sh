
# This command will be part of Buildspec.yml file
# The CICD pipeline will retrive code from git and deploy on the Script buckets.
aws s3 cp glue_gold_layer_job.py s3://aegon-aws-demo-git-scripts/glue_scripts/

# The CICD pipeline will invoke cloudformation scripts to deploy the glue jobs
aws cloudformation deploy  --template-file gluejob_cfn_create.yml  --stack-name aws-demo-glue-job  --capabilities CAPABILITY_NAMED_IAM
#aws cloudformation delete-stack   --stack-name aws-demo-glue-job
