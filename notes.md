aws cloudformation create-stack --stack-name csv-processing-stack --template-body file://cloud_stack.yaml --capabilities CAPABILITY_NAMED_IAM

aws cloudformation update-stack \
    --stack-name csv-processing-stack \
    --template-body file://cloud_stack.yaml  \
    --capabilities CAPABILITY_NAMED_IAM


    aws cloudformation create-stack --stack-name csv-processing-stack --template-body file://cloud_stack.yaml  --capabilities CAPABILITY_NAMED_IAM