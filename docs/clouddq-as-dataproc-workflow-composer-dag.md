# Usage Guide - Cloud Data Quality Engine - Run CloudDQ with Dataproc Workflows using Cloud Composer

In this example, we will create a scheduled Cloud Composer DAG to instantiate a
[Dataproc Workflow Template](https://cloud.google.com/dataproc/docs/concepts/workflows/overview)
to run the CloudDQ CLI.

Please note the following:

- we will be using the self-contained CloudDQ Python executable zip in the Dataproc jobs
- the steps in this usage guide have been tested on a Debian / Ubuntu Linux operating system
- all the commands in this usage guide must be executed from the top-level root directory of the cloned repository

## Before you begin

- Ensure that you have [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed on your system
- Ensure that you have `git`, `wget` and `zip` installed on your system
- Ensure you have [created a GCP Project with a GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
- Set the default Project ID used by `gcloud`
  - If you encounter the issue `No service account scopes specified` with the command below then run  `gcloud auth login` to obtain new credentials and try again

    ```bash
    gcloud config set project ${PROJECT_ID}
    ```

- Ensure the project has BigQuery, Dataproc, GCS and Cloud Composer APIs enabled:

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable composer.googleapis.com
```

Note that we will use the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account) `[project-number]-compute@developer.gserviceaccount.com` for the Dataproc and Composer VMs in this example.

To use a custom service account for Dataproc, follow the instructions [here for Dataproc](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#creating_a_cluster_with_a_user-managed_vm_service_account).

- Ensure that the custom service account for Dataproc has at minimum IAM permissions
  1) `roles/storage.objectAdmin` over the GCS Bucket `$GCS_BUCKET_NAME` that will be used to stage the CloudDQ configs and executables
  2) `roles/bigquery.dataEditor` over the GCP BigQuery Dataset in the environment variable `CLOUDDQ_BIGQUERY_DATASET` that will be managed by CloudDQ
- You will also need to update the `.scripts/dataproc-workflow-composer/dataproc_workflow_template.yaml` YAML file by adding `serviceAccount: <string_value>` under `placement.managedCluster.config.gceClusterConfig` as per [ClusterConfig](https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig) for Dataproc.

## 1. Configure project environment

Set environment variables.

```bash
export CLOUDDQ_RELEASE_VERSION=0.3.1  # CloudDQ Release Tag version to use for retrieving the relevant artifact
export PROJECT_ID=$(gcloud config list --format 'value(core.project)')  # GCP project ID where the Dataproc cluster and BigQuery dataset will be deployed
export REGION=<preferred_gcp_region>  # name of the GCP region where the Dataproc cluster is deployed
export ZONE=<preferred_gcp_zone>  # name of the specific zone inside $REGION where the Dataproc cluster is deployed
export GCS_BUCKET_NAME=<gcs_bucket_name>  # we will stage the executables and configs in this GCS bucket for deployment
export VPC_NETWORK=default  # optional: replace with a custom VPC to deploy your Dataproc cluster.
export CLOUDDQ_BIGQUERY_DATASET=cloud_data_quality  # optional: replace with a different BigQuery dataset name that will contain the BigQuery views corresponding to each rule_binding as well as the `dq_summary` validation outcome table.
export CLOUDDQ_BIGQUERY_REGION=EU  # optional: replace with a different BigQuery region where the data validation jobs will be created
export COMPOSER_ENVIRONMENT_NAME=clouddq-composer  # replace with a name that you want to use for Cloud Composer environment
export DATAPROC_CLUSTER_NAME=<cluster_name>  # name of the Dataproc cluster that will be used for deployment
export DATAPROC_WORKFLOW_NAME=clouddq-workflow  # replace with a name that you want to use for Dataproc Workflow template
```

## 2. Clone project

First clone the project if you haven't already done it:

```bash
git clone -b "v${CLOUDDQ_RELEASE_VERSION}" https://github.com/GoogleCloudPlatform/cloud-data-quality.git
```

## 3. Prepare the `CloudDQ` configuration files

Edit the `entities` config to use your [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and custom `CLOUDDQ_BIGQUERY_DATASET`:

```bash
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g ./configs/entities/test-data.yml
sed -i s/dq_test/${CLOUDDQ_BIGQUERY_DATASET}/g ./configs/entities/test-data.yml
```

## 4. Create Cloud Storage Bucket for CloudDQ

This is the bucket where the Python executables and the configuration files will be pulled from for the Dataproc PySpark job.

```bash
gsutil mb -p ${PROJECT_ID} -l ${REGION} -b on gs://${GCS_BUCKET_NAME}
```

Ensure you have sufficient IAM privileges to create Cloud Storage Buckets in your project.

## 5. Create test data

Create the corresponding test table `contact_details` mentioned in the entities config `configs/entities/test-data.yml` by running:

```bash
bq mk --force=true --project_id=${PROJECT_ID} --location=${CLOUDDQ_BIGQUERY_REGION} ${CLOUDDQ_BIGQUERY_DATASET}
bq load --project_id=${PROJECT_ID} --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details ./tests/data/contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

You can check to see if the test data has been loaded:

```bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.contact_details" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql --format=json
```

## 6. Upload Cloud DQ Artifacts to Cloud Storage

Run the following from the `cloud-data-quality` directory:

```bash
./scripts/dataproc-workflow-composer/upload_clouddq_to_gcs.sh
```

The above script will perform the following steps:

- create a zip file containing the `config` directory and stage it in the GCS bucket at `$GCS_BUCKET_NAME` to be retrieved by Dataproc
- stage the executable with the [Github Release](https://github.com/GoogleCloudPlatform/cloud-data-quality/releases) version specified in the environment variable `$CLOUDDQ_RELEASE_VERSION` in the GCS bucket at `$GCS_BUCKET_NAME` to be retrieved by Dataproc
- PySpark job located at `$GCS_BUCKET_NAME/clouddq_pyspark_driver.py`, will be submitted to the Dataproc cluster

## 7. Schedule CloudDQ jobs via Dataproc Workflow Templates using Cloud Composer

The Cloud Composer DAG in the below example are scheduled for execution with 15 minutes intervals.

### 7.1. Create a Cloud Composer Environment

Execute the following command taken from [Creating Cloud Composer Environments](https://cloud.google.com/composer/docs/how-to/managing/creating) for a "basic setup":

```bash
gcloud composer environments create ${COMPOSER_ENVIRONMENT_NAME} \
    --location ${REGION}
```

Please note that the approximate time to create an environment is 25 minutes.

Open the [Open the Environments page](https://console.cloud.google.com/composer/environments).

### 7.2. Update variables in the Dataproc Workflow Template YAML file with your project configuration

```bash
export CLOUDDQ_CONFIG=ALL
export WORKFLOW_TEMPLATE_FILE="scripts/dataproc-workflow-composer/dataproc_workflow_template.yaml"

sed -i s/\<clouddq_release_version\>/${CLOUDDQ_RELEASE_VERSION}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<preferred_gcp_region\>/${REGION}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<preferred_gcp_zone\>/${ZONE}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<gcs_bucket_name\>/${GCS_BUCKET_NAME}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<vpc_network\>/${VPC_NETWORK}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<dataproc_cluster_name\>/${DATAPROC_CLUSTER_NAME}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<clouddq_config\>/${CLOUDDQ_CONFIG}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<cloudd_bigquery_dataset\>/${CLOUDDQ_BIGQUERY_DATASET}/g ${WORKFLOW_TEMPLATE_FILE}
sed -i s/\<cloudd_bigquery_region\>/${CLOUDDQ_BIGQUERY_REGION}/g ${WORKFLOW_TEMPLATE_FILE}
```

You can read more about using YAML files with Dataproc Workflow Templates in [HERE](https://cloud.google.com/dataproc/docs/concepts/workflows/using-yamls).

### 7.3. Import the workflow template file to Dataproc Workflow Templates

```bash
gcloud dataproc workflow-templates import ${DATAPROC_WORKFLOW_NAME} \
    --source=${WORKFLOW_TEMPLATE_FILE} \
    --region=${REGION}
```

You can see your Dataproc Workflow Templates in the [GCP Web Console](https://pantheon.corp.google.com/dataproc/workflows/templates)

### 7.4. Update variables in the DAG with your project configuration

```bash
export DAG_PY_FILE="scripts/dataproc-workflow-composer/clouddq_composer_dataproc_workflow_job.py"

sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g ${DAG_PY_FILE}
sed -i s/\<preferred_gcp_region\>/${REGION}/g ${DAG_PY_FILE}
sed -i s/\<template_id\>/${DATAPROC_WORKFLOW_NAME}/g ${DAG_PY_FILE}
```

### 7.5. Copy DAG file to Composer bucket

```bash
export DAG_BUCKET=$(gcloud composer environments describe --format="value(config.dagGcsPrefix)" \
    --project ${PROJECT_ID} --location ${REGION} ${COMPOSER_ENVIRONMENT_NAME})

gsutil cp ${DAG_PY_FILE} ${DAG_BUCKET}
```

## 8. Check Airflow job status

Get the link for the Airflow UI and open it in your browser:

```bash
gcloud composer environments describe --format="value(config.airflowUri)" \
    --project ${PROJECT_ID} --location ${REGION} ${COMPOSER_ENVIRONMENT_NAME}
```

You can also check the status of the Dataproc Workflow in the [GCP Web Console](https://pantheon.corp.google.com/dataproc/workflows/instances).

## 9. Check the CloudDQ results

Once the Composer DAG execution is successfully finished, you can see the DQ validation outcomes in the BigQuery table `dq_summary` by running:

```bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.dq_summary" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql --format=json
```

## 10. Clean up

To avoid incurring unnecessary costs, ensure you delete all resources created in this guide including:

- Cloud Storage Buckets
- BigQuery Datasets
- Cloud Composer Cluster

## Improvements / Feedbacks

If you encounter an issue with any of the above steps or have any feedback, please feel free to create a github issue or contact clouddq@google.com.
