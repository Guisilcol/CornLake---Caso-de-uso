# Author: Guilherme dos Santos Magalhães 
# Date: 2021-02-21
# Description: Build and deploy the lambda function. 
# Requirements: SAM CLI, AWS CLI, Poetry. Don't forget to configure the template.yaml file with de values of prod environment variables.
# Usage: ./build.sh


RESOURCE_NAME=DataGenerator
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

poetry export -f requirements.txt --output "$RESOURCE_NAME/requirements.txt" && \
sam build --base-dir $DIR && sam deploy --guided
