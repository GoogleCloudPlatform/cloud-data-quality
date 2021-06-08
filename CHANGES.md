# [0.2.0] - Unreleased

## Highlights
* simplifying arguments requirements in CLI
* CLI now uses default package templates instead of assuming the templates exist in a system path. This makes it easier to deploy the CLI as a single executable across environments.

## New Features / Improvements
* CLI now no longer requires explicitly passing in `--dbt_path` and `--dbt_profiles_dir` arguments. If not present, the CLI will copy a templated `dbt` directory path to the current working directory.
* CLI now defaults to using `dev` dbt profile name for `--environment_target`.

## Fixes
* [#35](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/35) incremental validation assumes the table `dq_summary` already exists.

## Breaking Changes
* The top-level `macros` directory is moved into the `dbt` directory. Users should not have to edit this file. However if customization is required, users currently using the top-level`macros` directory should switch to use the path `dbt/macros` instead.
* the file `dbt_project.yml` is now moved into the `dbt` directory. Users should not have to edit this file. However if customization is required, users should switch to use the file located at `dbt/dbt_project.yml` instead.
* `utils.run_dbt()` now expects the first argument `dbt_path` to be provided to the top-level `dbt` directory. This should not affect users since it is not exposed as an user-facing API.

## Deprecations

## Known Issues
* incremental validation assumes the table `dq_summary` already exists.

# [0.1.0] - Current Rolling Version

This is the release at tag version 0.1.0, created to allow users to return to this version in case they do not want to immediately upgrade to 0.2.0.

## Highlights

## New Features / Improvements

## Fixes

## Breaking Changes

## Deprecations

## Known Issues
* [#34](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/34): CLI not working if installed using `pip install .`
* [#35](https://github.com/GoogleCloudPlatform/cloud-data-quality/issues/35) incremental validation assumes the table `dq_summary` already exists.