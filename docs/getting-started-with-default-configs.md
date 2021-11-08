# Usage Guide

## Testing the CLI with the default configurations

This section of the docs walks through step-by-step on how to test CloudDQ using the default configurations.

Note the following assumes you have already met the project dependencies outlined in the main [README.md](../README.md#installing)

### 1. Project Set-Up

First clone the project:
```bash
export CLOUDDQ_RELEASE_VERSION="0.3.1"
git clone -b "v${CLOUDDQ_RELEASE_VERSION}"  https://github.com/GoogleCloudPlatform/cloud-data-quality.git
cd cloud-data-quality
```

Ensure you have created a [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) created for running the Data Quality Validation jobs.

We then set the project ID as an enviroment variable and as the main project used by `gcloud`:
```bash
export PROJECT_ID=<replace_with_your_gcp_project_id>
gcloud config set project ${PROJECT_ID}
```

If you encounter the issue `No service account scopes specified` with the above command, run  `gcloud auth login` to obtain new credentials and try again.

Ensure the project has BigQuery API enabled:
```
gcloud services enable bigquery.googleapis.com
```

### 2. Creating connection profiles to BigQuery

Create the `profiles.yml` ([details here](../README.md#setting-up-`dbt`)) config to connect to BigQuery:
```bash
cp dbt/profiles.yml.template dbt/profiles.yml
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g dbt/profiles.yml
```

You can set the environment variable `CLOUDDQ_BIGQUERY_DATASET` to customize the BigQuery dataset name that will contain the BigQuery views corresponding to each rule_binding as well as the `dq_summary` validation outcome table:
```bash
export CLOUDDQ_BIGQUERY_DATASET=cloud_data_quality
```

This environment variable will be automatically picked up by `dbt` from the `profiles.yml` config file.

You can also set the environment variable `CLOUDDQ_BIGQUERY_REGION` to customize the BigQuery region where the BigQuery dataset and BigQuery data validation jobs will be created:
```bash
export CLOUDDQ_BIGQUERY_REGION=EU
```

If you are using OAuth in the `profiles.yml` to authenticate to GCP, ensure you are logged in to `gcloud` with [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/production):
```bash
gcloud auth application-default login
```

If you are explicitly providing a service-acount json key to `profiles.yml` for authentication, you don't need to worry about the above step.

### 3. Prepare `CloudDQ` configuration files

Edit the `entities` config to use your [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and custom `CLOUDDQ_BIGQUERY_DATASET`:
```bash
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g configs/entities/test-data.yml
sed -i s/dq_test/${CLOUDDQ_BIGQUERY_DATASET}/g configs/entities/test-data.yml
```

### 4. Install `CloudDQ`

Install `CloudDQ` in a virtualenv using the instructions in [Installing from source](../README.md#installing-from-source). Then test whether you can run the CLI by running:
```
python3 clouddq --help
```

Alternatively, you can download a pre-built zip artifact for `CloudDQ` by running:
```
wget -O clouddq_executable_v0.2.1.zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v0.2.1/clouddq_executable_v0.2.1_linux-amd64.zip
```

Currently, we only provide the self-contained executable zip artifact for running on debian/ubuntu linux systems. The artifact will not work on MacOS/Windows.

Once completed you can use the CLI by passing the zip executable into any Python interpreter:
```
python3 clouddq_executable_"v${CLOUDDQ_RELEASE_VERSION}".zip --help
```

### 5. Create test data

Create the corresponding test table `contact_details` mentioned in the entities config `configs/entities/test-data.yml` by using `bq load` :
```bash
bq mk --location=${CLOUDDQ_BIGQUERY_REGION} ${CLOUDDQ_BIGQUERY_DATASET}
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details tests/data/contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

### 6. Run the CLI

Run the following command to execute the rule_bindings `T2_DQ_1_EMAIL` in `configs/rule_bindings/team-2-rule-bindings.yml`:
```
python3 clouddq \
    T2_DQ_1_EMAIL \
    configs \
    --metadata='{"test":"test"}' \
    --dbt_profiles_dir=dbt \
    --dbt_path=dbt \
    --environment_target=dev
```

Or if you are using the pre-built zip file (only works on linux systems such as Debian/Ubuntu):

```
python3 "clouddq_executable_v${CLOUDDQ_RELEASE_VERSION}".zip \
    T2_DQ_1_EMAIL \
    configs \
    --metadata='{"test":"test"}' \
    --dbt_profiles_dir=dbt \
    --dbt_path=dbt \
    --environment_target=dev
```

By running this CLI command, `CloudDQ` will:
1. convert the YAML configs in `T2_DQ_1_EMAIL` into a SQL file located at `dbt/models/rule_binding_views/T2_DQ_1_EMAIL.sql`
2. validate that the SQL is valid using BigQuery dry-run feature
3. create a BigQuery view using this SQL file in the BigQuery dataset specified in `profiles.yml`
4. create a BigQuery job to execute the SQL in this view. The BigQuery job will be created in the GCP project specified in `profiles.yml`
5. aggregate the validation outcomes using the logic in `dbt/models/data_quality_engine/main.sql`
6. write the Data Quality validation results into a table called `dq_summary`.

The `dq_summary` table will be automatically created by `CloudDQ` at the GCP Project, BigQuery Dataset, and BigQuery Region specified in `profiles.yml`.

### 7. Check the results

To see the result DQ validation outcomes in the BigQuery table `dq_summary`, run:
```bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.dq_summary" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql --format=json
```

### 8. Clean up

To avoid incurring unnecessary costs, ensure you delete all resources created in this guide.

## Improvements / Feedbacks

If you encounter an issue with any of the above steps or have any feedback, please feel free to create a github issue or contact clouddq@google.com.
