# rust-gcp

## Steps to build and run
- Create a new gcp project (if you don't already have one)
- Enable Billing
- Authenticate your gcloud SDK
    > gcloud auth login
- Set your project
    > gcloud config set project getcloudy-469014
- Enable Required Google Cloud APIs
    > gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com
- Build the container image
    > gcloud builds submit --config cloudbuild.yaml .
    - To build an image with a specific tag:
        > gcloud builds submit --tag gcr.io/getcloudy-469014/rust-gcp-image:NEW_TAG
- Deploy the container image
    > gcloud run deploy rust-gcp --image gcr.io/getcloudy-469014/rust-gcp-image:latest --region asia-south2