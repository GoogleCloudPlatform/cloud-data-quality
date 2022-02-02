# Cloud Data Quality Engine

[![beta](https://badgen.net/badge/status/beta/1E90FF)](https://badgen.net/badge/status/beta/1E90FF)
[![build-test status](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml/badge.svg)](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introductions

`CloudDQ` is a cloud-native, declarative, and scalable Data Quality validation Command-Line Interface (CLI) application for Google BigQuery.

It takes as input Data Quality validation tests defined using a flexible and reusable YAML configurations language. For each `Rule Binding` definition in the YAML configs, `CloudDQ` creates a corresponding SQL view in your Data Warehouse. It then executes the view and collects the data quality validation outputs into a summary table for reporting and visualization.

`CloudDQ` currently supports in-place validation of BigQuery data.

* For a high-level overview of the purpose of `CloudDQ`, an explanation of the concepts and how it works, as well as how you would consume the outputs, please see our [Overview](OVERVIEW.md)
* For tutorials on how to use `CloudDQ`, example use cases, deployment best practices and example dashboards, see the [User Manual](USERMANUAL.md)
* We also provide a [Reference Guide](REFERENCE.md) with spec of the configuration and the library reference.
* For more advanced rules covering more specific requirements, please refer to [Advanced Rules User Manual](docs/examples/advanced_rules/USERMANUAL.md).

**Note:** This project is currently in beta status and may still change in breaking ways.

## Contributions

We welcome all community contributions, whether by opening Github Issues, updating documentations, or updating the code directly. Please consult the [contribution guide](CONTRIBUTING.md) for details on how to contribute. 

Before opening a pull request to suggest a feature change, please open a Github Issue to discuss the use-case and feature proposal with the project maintainers.


## Feedback / Questions

For any feedback or questions, please feel free to get in touch  at `clouddq` at `google.com`.


## License

CloudDQ is licensed under the Apache License version 2.0. This is not an official Google product.
