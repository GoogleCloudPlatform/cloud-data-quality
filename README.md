# Cloud Data Quality Engine

[![GA](https://badgen.net/badge/status/GA/1E90FF)](https://badgen.net/badge/status/GA/1E90FF)
[![build-test status](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml/badge.svg)](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introductions

CloudDQ is a cloud-native, declarative, and scalable Data Quality validation Command-Line Interface (CLI) application for Google BigQuery. CloudDQ allows users to define and schedule custom Data Quality checks across their BigQuery tables. Data Quality validation results will be available in another BigQuery table of their choice. Users can then build dashboards to routinely monitor data quality from the dashboards.

Key properties:
- Declarative rules configuration and support for CI/CD
- In-place validation without extracting data for both BigQuery-native tables and GCS structured data via BigQuery External Tables, benefitting from BigQuery performance and scalability while minimising security risk surface
- Validation results endpoints designed for programmatic consumption (persisted BigQuery storage and Cloud Logging sink), allowing custom integrations such as with BI reporting and metadata management tooling.

CloudDQ takes as input Data Quality validation tests defined using declarative YAML configurations. Data Quality `Rules` can be defined using custom BigQuery SQL logic with parametrization to support complex business rules and reuse. For each `Rule Binding` definition in the YAML configs, CloudDQ creates a corresponding SQL view in BigQuery. CloudDQ then executes the view using BigQuery SQL Jobs and collects the data quality validation outputs into a summary table for reporting and visualization.

Data Quality validation execution consumes [BigQuery slots](https://cloud.google.com/bigquery/docs/slots). BigQuery Slots can be provisioned on-demand for each run, in which case you pay for the [data scanned by the queries](https://cloud.google.com/bigquery/pricing#on_demand_pricing). For production usage, we recommend using [dedicated slots reservations](https://cloud.google.com/bigquery/docs/reservations-intro) to benefit from predictable [flat-rate pricing](https://cloud.google.com/bigquery/pricing#flat-rate_pricing).

We recommend using [Dataplex Data Quality Task](https://cloud.google.com/dataplex/docs/check-data-quality) to deploy CloudDQ. Dataplex Data Quality Task provides a managed and serverless deployment, automatic upgrades, and native support for task scheduling.

* For a high-level overview of the purpose of CloudDQ, an explanation of the concepts and how it works, as well as how you would consume the outputs, please see our [Overview](OVERVIEW.md)
* For tutorials on how to use CloudDQ, example use cases, and deployment best practices, see the [User Manual](USERMANUAL.md)
* We also provide a [Reference Guide](REFERENCE.md) with spec of the configuration and the library reference.
* For more advanced rules covering more specific requirements, please refer to [Advanced Rules User Manual](docs/examples/advanced_rules/USERMANUAL.md).

## Contributions

We welcome all community contributions, whether by opening Github Issues, updating documentations, or updating the code directly. Please consult the [contribution guide](CONTRIBUTING.md) for details on how to contribute. 

Before opening a pull request to suggest a feature change, please open a Github Issue to discuss the use-case and feature proposal with the project maintainers.


## Feedback / Questions

For any feedback or questions, please feel free to get in touch  at `clouddq` at `google.com`.


## License

CloudDQ is licensed under the Apache License version 2.0. This is not an official Google product.
