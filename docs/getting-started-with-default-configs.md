# Usage Guide

## Testing the CLI with the default configurations

This section of the docs walks through step-by-step on how to test CloudDQ using the default configurations.

Note the following assumes you have already met the project dependencies outlined in the main [README.md](../README.md#installing)

### Project Set-Up

First clone the project:
```
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality.git
cd cloud-data-quality
```

Ensure you have a [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) created for running the Data Quality Validation jobs. We then set the project ID as an enviroment variable and as the main project used by `gcloud`:
```bash
export GCP_PROJECT_ID=<replace_with_your_gcp_project_id>
gcloud config set project ${GCP_PROJECT_ID}
```

If you encounter the issue `No service account scopes specified` with the above command, run  `gcloud auth login` to obtain new credentials and try again.

Ensure the project has BigQuery API enabled:
```
gcloud services enable bigquery.googleapis.com
```

### Creating connection profiles to BigQuery

Create the `profiles.yml` ([details here](../README.md#setting-up-`dbt`)) config to connect to BigQuery:
```bash
cp profiles.yml.template profiles.yml
sed -i s/\<your_gcp_project_id\>/${GCP_PROJECT_ID}/g profiles.yml
```

You can set the environment variable `DBT_GCP_DATASET` to customize the BigQuery dataset name that will contain the BigQuery views corresponding to each rule_binding as well as the `dq_summary` validation outcome table:
```bash
export DBT_GCP_DATASET=cloud_data_quality
```

This environment variable will be automatically picked up by `dbt` from the `profiles.yml` config file.

You can also set the environment variable `DBT_BIGQUERY_REGION` to customize the BigQuery region where the BigQuery dataset and BigQuery data validation jobs will be created:
```bash
export DBT_BIGQUERY_REGION=EU
```

If you are using OAuth in the `profiles.yml` to authenticate to GCP, ensure you are logged in to `gcloud` with [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/production):
```bash
gcloud auth application-default login
```

If you are explicitly providing a service-acount json key to `profiles.yml` for authentication, you don't need to worry about the above step.

### Prepare `CloudDQ` configuration files

Edit the `entities` config to use your [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and custom `DBT_GCP_DATASET`:
```bash
sed -i s/\<your_gcp_project_id\>/${GCP_PROJECT_ID}/g configs/entities/test-data.yml
sed -i s/dq_test/${DBT_GCP_DATASET}/g configs/entities/test-data.yml
```

### Install `CloudDQ`

Install `CloudDQ` in a virtualenv using the instructions in [Installing from source](../README.md#installing-from-source). Then test whether you can run the CLI by running:
```
python3 clouddq --help
```

Alternatively, you can build a standalone executable Python zip by running:
```
make build
```

This will take about 5 minutes to build the first time as `bazel` is fetching the Python interpreter as well as all of the project dependencies to be included in the Python zip executable.

Once completed you can use the CLI by passing the zip executable into any Python interpreter:
```
python bazel-bin/clouddq/clouddq_patched.zip --help
```

More details about this step can be found at the main [README.md](../README.md#build-a-self-contained-python-executable-with-bazel)

### Create test data

Create the corresponding test table `contact_details` mentioned in the entities config `configs/entities/test-data.yml` by running:
```
dbt seed --profiles-dir=.
```

### Run the CLI

Run the following command to execute the rule_bindings `T2_DQ_1_EMAIL` in `configs/rule_bindings/team-1-rule-bindings.yml`:
```
python3 clouddq \
    T2_DQ_1_EMAIL \
    configs \
    --metadata='{"test":"test"}' \
    --dbt_profiles_dir=. \
    --dbt_path=dbt \
    --environment_target=dev
```

`CloudDQ` will automatically convert the YAML configs in `T2_DQ_1_EMAIL` into a SQL file located at `dbt/models/rule_binding_views/T2_DQ_1_EMAIL.sql`. `CloudDQ` will then create a BigQuery view using this SQL file, create a BigQuery job in `GCP_PROJECT_ID` to execute the SQL in this view, aggregate the validation outcomes using the logic in `dbt/models/data_quality_engine/main.sql`, and finally writing the Data Quality validation results into a table called `dq_summary`. This table will be automatically created by `CloudDQ` at the locations `GCP_PROJECT_ID`, `DBT_GCP_DATASET`, and `DBT_BIGQUERY_REGION` specified in `profiles.yml`.

### Check the results

To see the result DQ validation outcomes in the BigQuery table `dq_summary`, run:
```bash
echo "select * from \`${GCP_PROJECT_ID}\`.${DBT_GCP_DATASET}.dq_summary" | bq query --location=${DBT_BIQUERY_REGION} --nouse_legacy_sql --format=json
```

### Improvements / Feedbacks

If you encounter an issue with any of the above steps or have any feedback, please feel free to create a github issue or contact clouddq@google.com.
