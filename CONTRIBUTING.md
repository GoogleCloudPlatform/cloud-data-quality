# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google/conduct/).



## Development

CloudDQ is currently only tested to run on `Ubuntu`/`Debian` linux distributions. It may not work properly on other OS such as `MacOS`, `Windows`/`cygwin`, or `CentOS`/`Fedora`/`FreeBSD`, etc...

For development or trying out CloudDQ, we recommend using either [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor) or a [Google Cloud Compute Engine VM](https://cloud.google.com/compute) with the [Debian 11 OS distribution](https://cloud.google.com/compute/docs/images/os-details#debian).

### Dependencies

* make: https://www.gnu.org/software/make/manual/make.html
* golang (for building [bazelisk](https://github.com/bazelbuild/bazelisk)): https://golang.org/doc/install
* Python 3.8.x or 3.9.x: https://wiki.python.org/moin/BeginnersGuide/Download
* gcloud SDK (for interacting with GCP): https://cloud.google.com/sdk/docs/install

From a `Ubuntu`/`Debian` machine, install the above dependencies by running the following script:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
source scripts/install_development_dependencies.sh
```

Building CloudDQ requires the command `python3` to point to a Python Interterpreter version 3.8.x or 3.9.x. To install the correct Python version, please refer to the script `scripts/poetry_install.sh` for an interactive installation or `scripts/install_python3.sh` for a non-interactive installation intended for automated build/test processes. 

### Building a self-contained executable from source

After installing all dependencies and building it, you can clone the latest version of the code and build it by running the following commands:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
cd cloud-data-quality
make build
```

We provide a `Makefile` with common development steps such as `make build` to create the artifact, `make test` to run tests, and `make lint` to apply linting over the project code.

The `make build` command will fetch `bazel` and build the project into a self-contained Python zip executable called `clouddq_patched.zip` located in the current path.

You can then run the resulting zip artifact by passing it into the same Python interpreter version used to build the executable (this will show the help text):

```bash
#!/bin/bash
python3 clouddq_patched.zip --help
```

This step will take a few minutes to complete. Once completed for the first time, the artifacts will be cached and subsequent builds will be much faster.

The executable Python zip is not cross-platform compatible. You will need to build the executable separately for each of your target platforms. e.g. an artifact built using a machine running Ubuntu-18 and Python version 3.9.x will not work when transfered to another machine with Ubuntu-18 and Python version 3.8.x or another machine with Debian-11 and Python version 3.9.x.

To speed up builds, you may want to update the [bazel cache](https://docs.bazel.build/versions/master/remote-caching.html#google-cloud-storage) in `.bazelrc` to a GCS bucket you have access to.
```
build --remote_cache=https://storage.googleapis.com/<your_gcs_bucket_name>
```

### Running the CLI from source without building

You may prefer to run the CLI directly without first building a zip executable. In which case, you can run `bazelisk run` to execute the main CLI.

First install Bazelisk into a local path:

```bash
#!/bin/bash
make bin/bazelisk
```

Then run the CloudDQ CLI using `bazelisk run` and pass in the CLI arguments after the `--`:

```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
export CLOUDDQ_TARGET_BIGQUERY_TABLE="<project-id>.<dataset-id>.<table-id>"
bin/bazelisk run //clouddq:clouddq -- \
  T2_DQ_1_EMAIL \
  $(pwd)/configs \
  --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
  --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
  --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
  --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}"  
```

Note that `bazel run` execute the code in a sandbox, therefore non-absolute paths will be relative to the sandbox path not the current path. Ensure you are passing absolute paths to the command line arguments, e.g. pass in `$(pwd)/configs` instead of 'configs'.


### Run tests and linting

To run all tests:

```bash
#!/bin/bash
make test
```

For integration testing, you must first set the environment variables outlined in `set_environment_variables.sh` before running  `make test`. For example:

```bash
#!/bin/bash
cp set_environment_variables.sh setenvs.sh
# Manually edit `setenvs.sh` to use your project configurations.
source setenvs.sh && make test
```

To run a particular test:

```bash
#!/bin/bash
make test test_templates
```

By default, running integration tests with Dataplex are skipped. To enable these tests, ensure the correct environments are set and run:

```bash
#!/bin/bash
make build  # create the relevant zip artifacts to be used in tests
make test -- --run-dataplex
```

To apply linting:

```bash
#!/bin/bash
make lint
```

To run build and tests on clean OS images, install [cloud-build-local](https://cloud.google.com/build/docs/build-debug-locally) and run:

```bash
#!/bin/bash
./setenvs.sh && ./scripts/cloud-build-local.sh  2>&1 | tee build.log
```

This will set the environment variables required for the run and pipe the run logs to a file called `build.log`.

This will run as your gcloud ADC credentials. If you are running on a GCE VM, ensure the Compute Engine service account has the access scope to use all GCP APIs.

To run cloud-build-local in dry-run mode:

```bash
#!/bin/bash
bash scripts/cloud-build-local.sh --dry-run
```

## Troubleshooting

### 1. Cannot find shared library dependencies on system
If running `make build` fails due to missing shared library dependencies (e.g. `_ctypes` or `libssl`), try running the below steps to install them, then clear the bazel cache with `make clean` and retry.

#### `Ubuntu`/`Debian`:

```bash
#!/bin/bash
source scripts/install_development_dependencies.sh
```

### 2. Wrong `glibc` version
If you encounter the following error when running the executable Python zip on a different machine to the one you used to build your zip artifact:

```
/lib/x86_64-linux-gnu/libm.so.6: version `GLIBC_2.xx' not found
```

This suggests that the `glibc` version on your target machine is incompatible with the version on your build machine. Resolve this by rebuilding the zip on machine with identical `glibc` version (usually this means the same OS version) as on your target machine, or vice versa.
