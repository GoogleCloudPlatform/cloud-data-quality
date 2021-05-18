# Usage Guide

## Testing the CLI with the default configurations

This section of the docs walks through step-by-step on how to deploy CloudDQ to validate both GCS and BigQuery data assets in a Google Dataplex Data Lake.

Note the following assumes you have already met the project dependencies outlined in the main [README.md](../README.md#installing).

In this guide, we will use a pre-packages `CloudDQ` executable from a public GCS bucket location. As such, there is no need to clone the repository or build the executable yourself.

## Prepare GCP Resources

### 0. Project set-up

Ensure you have created a [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin). 

We then set your GCP config variables and resource names to be used in the following steps:

```bash
# GCP Project Resource
export PROJECT_ID=<your_project_id>  # GCP project ID where the Dataproc cluster and BigQuery dataset will be deployed
export REGION=us-central1  # name of the GCP region where the Dataproc metastore and cluster will be deployed.
# Ensure your chosen GCP $REGION supports both Dataproc Metastore AND Dataplex. Currently, the following regions are supported:
# us-central1
# europe-west2
export ZONE=<preferred_gcp_zone>  # name of the specific zone inside $REGION where the Dataproc metastore and cluster will be deployed
export VPC_NETWORK=default  # optional: replace with a custom VPC to deploy your Dataproc cluster.

# Data Lake Resources
export HIVE_METASTORE_SERVICE_NAME=<hive_metastore_service_name>  # name of the Hive Metastore Service for the Data Lake
export HIVE_METASTORE_REFERENCE=projects/${PROJECT_ID}/locations/${REGION}/services/${HIVE_METASTORE_SERVICE_NAME}
export DATAPROC_CLUSTER_NAME=<cluster_name>  # name of the Dataproc cluster that will be used for deployment
export DATAPLEX_LAKE=<dataplex_lake_name>  # name of your Dataplex Lake
export DATAPLEX_ZONE=<dataplex_zone_name>  # name of your Dataplex Zone associated with $DATAPLEX_LAKE
export DATAPLEX_GCS_ASSET=<dataplex_gcs_bucket_asset>  # name of the GCS Bucket that will be attached to $DATAPLEX_ZONE
export DATAPLEX_BIGQUERY_ASSET=<dataplex_bigquery_dataset_asset>  # name of the BigQuery Dataset that will be attached to $DATAPLEX_ZONE. You must use underscores _ instead of dashes -.

# CloudDQ Resources
export GCS_BUCKET_NAME=<gcs_bucket_name>  # we will stage the executables and configs in this GCS bucket for deployment
export CLOUDDQ_BIGQUERY_DATASET=cloud_data_quality  # optional: replace with a different BigQuery dataset name that will contain the BigQuery views corresponding to each rule_binding as well as the `dq_summary` validation outcome table.
export CLOUDDQ_BIGQUERY_REGION=$REGION  # create BigQuery validation jobs in the same region as our Data Lake resources
```

Note that we will use the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account) `[project-number]-compute@developer.gserviceaccount.com` for the Dataproc VM in this example.

Now we will set the default project ID used by `gcloud`:
```
gcloud config set project ${PROJECT_ID}
```

If you encounter the issue `No service account scopes specified` with the above command, run  `gcloud auth login` to obtain new credentials and try again.

Ensure the relevant GCP service APIs are enabled in your project:
```
gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable metastore.googleapis.com
gcloud services enable dataplex.googleapis.com
```

### 1. Create Dataproc Metastore (DPMS)

Before creating our data lake, we must first create an instance of Dataproc Metastore (DPMS) to store metadata across our GCS and BigQuery tables. An existing DPMS instance is currently required for setting up a Dataplex lake. It could take up to 30 minutes to create a DPMS instance. 
 
Review the following [requirements](https://cloud.google.com/dataproc-metastore/docs/create-service#before-you-begin) and ensure you have the relevant IAM permissions outlined in [this page](https://cloud.google.com/dataproc-metastore/docs/quickstart-guide#access-control) before continuing.

```bash
gcloud metastore services create ${HIVE_METASTORE_SERVICE_NAME} \
    --location=${REGION} \
    --network=${VPC_NETWORK} \
    --tier=DEVELOPER \
    --hive-metastore-version=3.1.2 \
    > metastore.log 2>&1 & # send process to background so we can continue with the next steps
```

Detailed instructions for this step can be found at: https://cloud.google.com/dataproc-metastore/docs/quickstart-guide

As could take up to 30 minutes to create a Dataproc Metastore instance and the next step [Create Dataproc Cluster](#2.-create-dataproc-cluster) is dependent on this step, feel free to skip to steps 3,4,5 while waiting for steps 1 and 2 to complete.

You can monitor the status of this step by running:

```bash
gcloud metastore services list --project=${PROJECT_ID} --location=${REGION}
```

This step is complete when the the STATE of the hive metastore instance you just created is listed as "ACTIVE".

### 2. Create Dataproc Cluster

Once the Dataproc Metastore have been created successfully, we create a single-node Dataproc cluster connected to our Hive Metastore in order to run our CloudDQ job.

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
  --dataproc-metastore=${HIVE_METASTORE_REFERENCE} \
  > dataproc.log 2>&1 & # send process to background so we can continue with the next steps
```

Ensure you have the IAM role `roles/dataproc.editor` to create a Dataproc cluster.

Detailed instructions for this step can be found at: https://cloud.google.com/dataproc/docs/guides/create-cluster

Note that this step may take a few minutes to complete. Feel free to proceed with the next steps while waiting for this step to complete.

You can check whether the service has created successfully by running:

```bash
gcloud beta dataproc clusters list --project=${PROJECT_ID} --region=${REGION}
```

### 3. Create test data

We will now create some test data in both BigQuery and GCS. We will use the same CSV file and both data assets will be added to the Dataplex Data Lake.

Ensure you have sufficient IAM privileges to create GCS buckets & objects and BigQuery datasets & tables in your project.

First, create the GCS data:

```bash
gsutil mb -b on -l ${REGION} -p ${PROJECT_ID} gs://${DATAPLEX_GCS_ASSET}
gsutil cp gs://clouddq-maxis/contact_details.csv gs://${DATAPLEX_GCS_ASSET}/
```

Then create the BigQuery data:

```bash
bq mk --location=${REGION} ${DATAPLEX_BIGQUERY_ASSET}
bq load --location=${REGION} --source_format=CSV --autodetect ${DATAPLEX_BIGQUERY_ASSET}.contact_details gs://${DATAPLEX_GCS_ASSET}/contact_details.csv
```

### 4. Create a Dataplex Data Lake

Now we will create a [Dataplex](https://cloud.google.com/dataplex/docs/overview) Data Lake and attach the data resources we just created to the Lake for validation by CloudDQ.

Follow the following instuctions to create a Dataplex Lake, a Dataplex Zone, and attaching the GCS bucket and BigQuery dataset we've just created to this Zone: https://cloud.google.com/dataplex/docs/quickstart-guide#creating-a-lake.

Ensure you are using the same values as the environment we created earlier:

```bash
echo $DATAPLEX_LAKE
echo $DATAPLEX_ZONE
echo $DATAPLEX_GCS_ASSET
echo $DATAPLEX_BIGQUERY_ASSET
```

When creating the zone, ensure you enable metadata discovery and publishing. Specifically under metadata publishing, both checkboxes "Publish discovered tables to a metastore database" and "Publish discovered tables to a BigQuery dataset" must be enabled. We recommend using the default settings for the dataset/database names.

## Prepare CloudDQ configs

#### 5 Creating connection profiles to BigQuery

Create the `profiles.yml` ([details here](../README.md#setting-up-`dbt`)) config to connect to BigQuery:
```bash
bq mk --location=${REGION} ${CLOUDDQ_BIGQUERY_DATASET}
gsutil cp gs://clouddq-maxis/profiles.yml.template .
cp profiles.yml.template profiles.yml
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g profiles.yml
sed -i s/clouddq/${CLOUDDQ_BIGQUERY_DATASET}/g profiles.yml
sed -i s/EU/${CLOUDDQ_BIGQUERY_REGION}/g profiles.yml
```

We are using `method: oauth` in the `profiles.yml` to connect to GCP using the default profile name `dev`. This means that on Dataproc, the job will authenticate to GCP using the Dataproc VM service account (which will be the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account) if you have not created the Dataproc cluster with a custom service account).

### 6. Prepare the `CloudDQ` configuration files

First, we'll create an entity YAML config for the BigQuery asset `$DATAPLEX_BIGQUERY_ASSET`:

```bash
cat <<EOF > configs/entities/bigquery_table.yml   
entities:
  BIGQUERY_TABLE:
    source_database: BIGQUERY
    table_name: contact_details
    dataset_name: ${DATAPLEX_BIGQUERY_ASSET}
    project_name: ${PROJECT_ID}
    columns:
      ROW_ID:
        name: row_id
        data_type: STRING
        description: |-
          unique identifying id
      CONTACT_TYPE:
        name: contact_type
        data_type: STRING
        description: |-
          contact detail type
      VALUE:
        name: value
        data_type: STRING
        description: |-
          contact detail
      TS:
        name: ts
        data_type: DATETIME
        description: |-
          updated timestamp
EOF
```

Then we'll create another entity YAML config for the GCS asset `$DATAPLEX_GCS_ASSET`:

```bash
cat <<EOF > configs/entities/dataplex_table.yml   
entities:
  DATAPLEX_TABLE:
    source_database: DATAPLEX
    table_name: contact_details
    zone_name: ${DATAPLEX_ZONE}
    lake_name: ${DATAPLEX_LAKE}
    project_name: ${PROJECT_ID}
    columns:
      ROW_ID:
        name: row_id
        data_type: STRING
        description: |-
          unique identifying id
      CONTACT_TYPE:
        name: contact_type
        data_type: STRING
        description: |-
          contact detail type
      VALUE:
        name: value
        data_type: STRING
        description: |-
          contact detail
      TS:
        name: ts
        data_type: DATETIME
        description: |-
          updated timestamp
EOF
```

Now we'll create a few rule types:

```bash
cat <<EOF > configs/rules/dq_rules.yml   
rules:
  NOT_NULL_SIMPLE:
    rule_type: NOT_NULL

  CUSTOM_SQL_LENGTH_LE_30:
    rule_type: CUSTOM_SQL_EXPR
    params:
      custom_sql_expr: |-
        LENGTH( \$column ) <= 30

  NO_DUPLICATES_IN_COLUMN_GROUPS:
    rule_type: CUSTOM_SQL_STATEMENT
    params:
      custom_sql_arguments:
        - column_names
      custom_sql_statement: |-
        select
          \$column_names
        from data
        group by \$column_names
        having count(*) > 1
EOF
```

We'll create a default filter config that does not implement any filtering (i.e CloudDQ will scan all rows everytime with this config):

```bash
cat <<EOF > configs/row_filters/filters.yml   
row_filters:
  NONE:
    filter_sql_expr: |-
      True
EOF
```

We will now create two identical rule bindings, one for the Dataplex table on GCS and one for the BigQuery table:

```bash
cat <<EOF > configs/rule_bindings/rule_bindings.yml   
rule_bindings:
  BIGQUERY_TABLE_VALUE_NOT_NULL:
    entity_id: BIGQUERY_TABLE
    column_id: VALUE
    row_filter_id: NONE
    rule_ids:
      - NOT_NULL_SIMPLE
      - CUSTOM_SQL_LENGTH_LE_30
      - NO_DUPLICATES_IN_COLUMN_GROUPS:
          column_names: "contact_type,value"

  DATAPLEX_TABLE_VALUE_NOT_NULL:
    entity_id: DATAPLEX_TABLE
    column_id: VALUE
    row_filter_id: NONE
    rule_ids:
      - NOT_NULL_SIMPLE
      - CUSTOM_SQL_LENGTH_LE_30
      - NO_DUPLICATES_IN_COLUMN_GROUPS:
          column_names: "contact_type,value"
EOF
```

## Run CloudDQ

### 7. Deploy CloudDQ as Dataproc Job

Finally, we will submit a Dataproc job by running the following utility script:

```bash
gsutil cp gs://clouddq-maxis/clouddq_pyspark_driver.py .
gsutil cp gs://clouddq-maxis/dataplex-submit-dataproc-job.sh .
bash dataplex-submit-dataproc-job.sh ALL dev
```

This script will:
1. create a zip file containing the `config` directory and stage it in the GCS bucket at `$GCS_BUCKET_NAME` to be retrieved by Dataproc
3. submit a PySpark job to the Dataproc cluster we created earlier using the Driver programme located at `clouddq_pyspark_driver.py` 
4. the PySpark job will run the ClouDQ CLI using the `profiles.yml` and YAML config we created

This will create a table `dq_summary` at the GCP Project, BigQuery Dataset, and BigQuery Region we specified in in `profiles.yml`.

The first argument to the script is the CLI argument `RULE_BINDING_IDS`. For example, you can execute only specific Rule Bindings instead of all of the Rule Bindings in the `configs` path by running:
```bash
bash scripts/dataproc/submit-dataproc-job.sh DATAPLEX_TABLE dev
```

The second argument to the script is the environment target specified in the `profiles.yml` config file. For example, you can run the same set of Rule Bindings but in a different environment target called `prod` by running:
```bash
bash scripts/dataproc/submit-dataproc-job.sh ALL prod
```

### 9. Check the results

To see the result DQ validation outcomes in the BigQuery table `dq_summary`, run:
```bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.dq_summary" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql --format=json
```

### 10. Clean up

To avoid incurring unnecessary costs, ensure you delete all resources created in this guide.

## Improvements / Feedbacks

If you encounter an issue with any of the above steps or have any feedback, please feel free to create a github issue or contact clouddq@google.com.
