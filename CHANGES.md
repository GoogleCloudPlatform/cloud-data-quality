# [0.2.0] - Unreleased

## Highlights
* simplifying arguments requirements in CLI
* CLI now uses default package templates instead of assuming the templates exist in a system path. This makes it easier to deploy the CLI as a single executable across environments.

## New Features / Improvements
* CLI now no longer requires explicitly passing in `--dbt_path` and `--dbt_profiles_dir` arguments. If not present, the CLI will copy a templated `dbt` directory path to the current working directory.
* CLI now defaults to using `dev` dbt profile name for `--environment_target`.

## Fixes
* [#34](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/34) - how loading in templates from bundled package locations.

## Breaking Changes
* The top-level `macros` directory is moved into the `dbt` directory.
* the file `dbt_project.yml` is now moved into the `dbt` directory.
* `utils.run_dbt()` now expects the first argument `dbt_path` to be provided to the top-level `dbt` directory.

## Deprecations

## Known Issues
* incremental validation assumes the table `dq_summary` already exists.

# [0.1.0] - Current Rolling Version

## Highlights

## New Features / Improvements

## Fixes

## Breaking Changes

## Deprecations

## Known Issues
* [#34](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/34): CLI not working if installed using `pip install .`
* [#35](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/35) incremental validation assumes the table `dq_summary` already exists.