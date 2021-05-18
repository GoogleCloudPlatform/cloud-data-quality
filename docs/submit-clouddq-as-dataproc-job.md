# Usage Guide

## Cloud Data Quality Engine - Deploy CloudDQ Job with Dataproc

In this example, we will create a [Dataproc](https://cloud.google.com/dataproc/docs/concepts/overview) job to run the CloudDQ CLI.

Note that for this step we will need to build a self-contained CloudDQ Python executable Zip to be used in the Dataproc job. Before continuing, ensure you have already installed the development dependencies outlined in the main [README.md](../README.md#Development)

### 1. Project Set-Up

First clone the project:
```
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality.git
cd cloud-data-quality
```

Ensure you have created a [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin). 

We then set your GCP config variables and resource names to be used in the following steps:

```bash
export PROJECT_ID=<your_project_id>  # GCP project ID where the Dataproc cluster and BigQuery dataset will be deployed
export REGION=<preferred_gcp_region>  # name of the GCP region where the Dataproc cluster is deployed
export ZONE=<preferred_gcp_zone>  # name of the specific zone inside $REGION where the Dataproc cluster is deployed
export DATAPROC_CLUSTER_NAME=<cluster_name>  # name of the Dataproc cluster that will be used for deployment
export GCS_BUCKET_NAME=<gcs_bucket_name>  # we will stage the executables and configs in this GCS bucket for deployment
export VPC_NETWORK=default  # optional: replace with a custom VPC to deploy your Dataproc cluster.
export CLOUDDQ_BIGQUERY_DATASET=cloud_data_quality  # optional: replace with a different BigQuery dataset name that will contain the BigQuery views corresponding to each rule_binding as well as the `dq_summary` validation outcome table.
export CLOUDDQ_BIGQUERY_REGION=EU  # optional: replace with a different BigQuery region where the data validation jobs will be created
```

Note that we will use the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account) `[project-number]-compute@developer.gserviceaccount.com` for the Dataproc VM in this example. 

To use a custom service account, follow the instructions [here](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#creating_a_cluster_with_a_user-managed_vm_service_account). Ensure the custom service account has at minimum IAM permissions 1) `roles/storage.objectAdmin` over the GCS Bucket `$GCS_BUCKET_NAME` that will be used to stage the CloudDQ configs and executables and 2) `roles/bigquery.dataEditor` over the GCP BigQuery Dataset in the environment variable `CLOUDDQ_BIGQUERY_DATASET` that will be managed by CloudDQ.

Now we will set the default project ID used by `gcloud`:
```
gcloud config set project ${PROJECT_ID}
```

If you encounter the issue `No service account scopes specified` with the above command, run  `gcloud auth login` to obtain new credentials and try again.

Ensure the project has BigQuery, Dataproc, and GCS API enabled:
```
gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com
```

### 2. Create Dataproc Cluster

We will now create a single-node Dataproc cluster to run the job. Feel free to ignore this step if you already have a Dataproc cluster created at `$DATAPROC_CLUSTER_NAME` for the CloudDQ job.

```bash
gcloud beta dataproc clusters create ${DATAPROC_CLUSTER_NAME} \
  --enable-component-gateway \
  --project ${PROJECT_ID} \
  --region ${REGION} \
  --zone ${ZONE} \
  --single-node \
  --master-machine-type n1-standard-4 \
  --master-boot-disk-size 500 \
  --image-version 2.0-ubuntu18 \
  & # send process to background so we can continue with the next steps
```

Ensure you have the IAM role `roles/dataproc.editor` to create a Dataproc cluster.

Detailed instructions for this step can be found at: https://cloud.google.com/dataproc/docs/guides/create-cluster

Note that this step may take a few minutes to complete, hence why we're doing this first while we prepare the CloudDQ configurations.

### 3. Creating connection profiles to BigQuery

Create the `profiles.yml` ([details here](../README.md#setting-up-`dbt`)) config to connect to BigQuery:
```bash
cp profiles.yml.template profiles.yml
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g profiles.yml
sed -i s/clouddq/${CLOUDDQ_BIGQUERY_DATASET}/g profiles.yml
sed -i s/EU/${CLOUDDQ_BIGQUERY_REGION}/g profiles.yml
```

Ensure you are using connection `method: oauth` in the `profiles.yml` and take note of the profiles name (the default profile we will use is `dev`). This means that the job will authenticate to GCP using the Dataproc VM service account (which will be the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account) if you have not created the Dataproc cluster with a custom service account).

### 4. Prepare the `CloudDQ` configuration files

Edit the `entities` config to use your [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and custom `CLOUDDQ_BIGQUERY_DATASET`:
```bash
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g configs/entities/test-data.yml
sed -i s/dq_test/${CLOUDDQ_BIGQUERY_DATASET}/g configs/entities/test-data.yml
```

### 5. Build Self-contained `CloudDQ` Python executable

We will now build a self-contained executable Python zip by running:
```
make build
```

This will take about 5 minutes to build the first time as `bazel` is fetching the Python interpreter as well as all of the project dependencies to be included in the Python zip executable.

Once completed you can try smoke-testing the the CLI by passing the zip executable into a Python interpreter:
```
python3 bazel-bin/clouddq/clouddq_patched.zip --help
```

More details about this step can be found at the main [README.md](../README.md#build-a-self-contained-python-executable-with-bazel)

### 6. Create test data

Create the corresponding test table `contact_details` mentioned in the entities config `configs/entities/test-data.yml` by running:
```bash
bq mk --location=${CLOUDDQ_BIGQUERY_REGION} ${CLOUDDQ_BIGQUERY_DATASET}
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details dbt/data/contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

### 7. Deploy CloudDQ as Dataproc Job

Finally, we will submit a Dataproc job by running the following utility script:

```bash
bash scripts/dataproc/submit-dataproc-job.sh ALL dev
```

This script will:
1. create a zip file containing the `config` directory and stage it in the GCS bucket at `$GCS_BUCKET_NAME` to be retrieved by Dataproc
2. stage the executable zip located at `bazel-bin/clouddq/clouddq_patched.zip` in the GCS bucket at `$GCS_BUCKET_NAME` to be retrieved by Dataproc
3. submit a PySpark job to the Dataproc cluster we created earlier using the Driver programme located at `scripts/dataproc/clouddq_pyspark_driver.py` 
4. the PySpark job will run the ClouDQ CLI using the `profiles.yml` and YAML config we created

This will create a table `dq_summary` at the GCP Project, BigQuery Dataset, and BigQuery Region we specified in in `profiles.yml`.

The first argument to the script is the CLI argument `RULE_BINDING_IDS`. For example, you can execute only specific Rule Bindings instead of all of the Rule Bindings in the `configs` path by running:
```bash
bash scripts/dataproc/submit-dataproc-job.sh T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE dev
```

The second argument to the script is the environment target specified in the `profiles.yml` config file. For example, you can run the same set of Rule Bindings but in a different environment target called `prod` by running:
```bash
bash scripts/dataproc/submit-dataproc-job.sh ALL prod
```

### 8. Check the results

To see the result DQ validation outcomes in the BigQuery table `dq_summary`, run:
```bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.dq_summary" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql --format=json
```

### 9. Clean up

To avoid incurring unnecessary costs, ensure you delete all resources created in this guide.

## Improvements / Feedbacks

If you encounter an issue with any of the above steps or have any feedback, please feel free to create a github issue or contact clouddq@google.com.
