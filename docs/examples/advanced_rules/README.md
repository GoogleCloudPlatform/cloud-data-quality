# Advanced Rules
## Introduction
As an extension to existing examples and quickstart overview of CloudDQ rule framework some more advanced rules have been prepared that address common data quality scenarios. The list is not full (will be kept updated) but contains most popular dimensions like `timeliness`, `correctness`, `integrity`, `conformity`, `completnes` just to name a few.
 

## Rules list 

### Alerting for failed / delayed data ingestion
**User Story**: As a Data Engineer, I would like to run a DQ rule that identifies if there was no data ingested in a period of time of a specified duration directly preceding the current moment.

source: [timeliness_delayed_ingestion.yaml](timeliness_delayed_ingestion.yaml)
TBD
### Define complex rule with record columns
source: [correctness_complex_rule.yaml](correctness_complex_rule.yaml)

### Comparing data volumes / record counts between two time periods
TBD
source: [timeliness_volumes_per_period.yaml](timeliness_volumes_per_period.yaml)

### Validating Integrity (against reference data)
TBD
source: [integrity_reference_data.yaml](integrity_reference_data.yaml)

### Validating Integrity (against set returned by subquery)
TBD
source: [integrity_subquery.yaml](integrity_subquery.yaml)

### Validating Conformity (against an interval)
TBD
source: [conformity_email_regex.yaml](conformity_email_regex.yaml)

### Validating Completeness
TBD
source: [completness_with_condition.yaml](completness_with_condition.yaml)

### Validating Uniqueness
TBD
source: [uniqueness_with_column_groups.yaml](uniqueness_with_column_groups.yaml)

### Validating Accuracy
TBD
source: [accuracy_distribution_based.yaml](accuracy_distribution_based.yaml)