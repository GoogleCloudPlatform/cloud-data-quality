# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PySpark Driver for launching CloudDQ as a Spark Job

This is designed to be launched using a companion script such
as `scripts/dataproc/submit-dataproc-job.sh`.
"""

from itertools import chain
from pathlib import Path
from pprint import pprint
from zipfile import ZipFile

import hashlib
import os
import subprocess
import sys

from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from contextlib import closing


def verify_executable(filename, expected_hexdigest):
    hash_sha256 = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    if not hash_sha256.hexdigest() == expected_hexdigest:
        raise ValueError(f"Cannot verify executable {filename}.")


def prepare_configs_path(input_directory):
    configs_path = Path("configs")
    if not configs_path.is_dir():
        print(f"Creating configs directory at: `{configs_path.absolute()}`")
        configs_path.mkdir()
    for filename in input_directory:
        file = Path(filename)
        # checking if it is a file
        if file.is_file():
            if file.suffix == ".zip" and file.name != "clouddq-executable.zip":
                print(
                    f"Extracting file {file} to configs directory `{configs_path}`..."
                )
                with ZipFile(file, "r") as zipObject:
                    zipObject.extractall(configs_path)
            elif file.suffix == ".yml" or file.suffix == ".yaml":
                print(
                    f"Copying YAML file {file} to configs directory `{configs_path}`..."
                )
                configs_path.joinpath(file.name).write_text(file.open().read())
        # else if it's a directory,
        # look for yaml/yml files in the path
        # and copy them to the `configs` directory
        elif file.is_dir():
            for yaml_file in chain(file.glob("**/*.yaml"), file.glob("**/*.yml")):
                try:
                    content = yaml_file.open().read()
                    configs_path.joinpath(yaml_file.name).write_text(content)
                except Exception as e:
                    print(f"Failed to parse config file: {yaml_file}\n{e}")
                    continue
    return configs_path


def main(args):
    with open(f"{args[1]}.hashsum") as f:
        expected_hexdigest = f.read().replace("\n", "").replace("\t", "")
        print(f"CloudDQ executable expected hexdigest: {expected_hexdigest}")
        verify_executable(args[1], expected_hexdigest)
    args[3] = str(Path("configs").absolute())
    cmd = f"{sys.executable} {' '.join(args[1:])}"
    print(f"Executing commands:\n {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def create_cloudq_staging_schema():
    try:
        master = "local"
        APPLICATION_NAME = "Create Clouddq Staging Schema"
        schema = "clouddq_staging_schema_test"
        summary_table_name = "dq_summary"
        spark = SparkSession.builder.master(master) \
            .appName(APPLICATION_NAME) \
            .enableHiveSupport() \
            .getOrCreate()
        sc = spark.sparkContext.getOrCreate()
        hc = SQLContext(sc, sparkSession=spark)
        print("Show databases:")
        spark.sql("SHOW DATABASES;").show()
        print("Create clouddq staging schema if not exists:")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema};")
        print("Show databases:")
        spark.sql("SHOW DATABASES;").show()
        spark.sql(f"use {schema}").show()
        print("Show tables:")
        spark.sql("SHOW TABLES;").show()
        print(f"CREATE {summary_table_name} table if not exists:")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {summary_table_name} (invocation_id string, execution_ts timestamp,"
                  " rule_binding_id string, rule_id string, table_id string, column_id string, dimension string,"
                  " metadata_json_string string, configs_hashsum string, dataplex_lake string, dataplex_zone string, "
                  "dataplex_asset_id string ,dq_run_id string ,progress_watermark boolean, rows_validated bigint, "
                  "complex_rule_validation_errors_count bigint, complex_rule_validation_success_flag boolean,"
                  "last_modified timestamp, success_count bigint, success_percentage double, failed_count bigint, "
                  "failed_percentage double, null_count bigint ,null_percentage double)").show()
        spark.sql("SHOW TABLES").show()
        print(f"Check if {summary_table_name} table exists:")
        if summary_table_name in hc.tableNames(schema):
            print("Table Exists!")
        else:
            print("Table does not exist :(")
    except Exception as e:
        print(e.with_traceback())
    finally:
        spark.stop()

if __name__ == "__main__":
    print("OS runtime details:")
    subprocess.run("cat /etc/*-release", shell=True, check=True)
    print("Python executable path:")
    print(sys.executable)
    print("Python Version:")
    print(sys.version_info)
    print("PySpark working directory:")
    pprint(Path().absolute())
    print("PySpark directory content:")
    pprint(os.listdir())
    print("Input PySpark arguments:")
    pprint(sys.argv)
    input_configs = sys.argv[3]
    print(f"User-specified CloudDQ YAML configs: {input_configs}")
    configs_path = prepare_configs_path(os.listdir())
    print("Configs directory contents is:")
    pprint(list(configs_path.glob("**/*")))
    print("Start thrift server:")
    cmd = f"""export HIVE_SERVER2_THRIFT_PORT=10005;
    export HIVE_SERVER2_THRIFT_BIND_HOST=localhost;
    /usr/lib/spark/sbin/start-thriftserver.sh"""
    print(f"Executing commands:\n {cmd}")
    subprocess.run(cmd, shell=True, check=True)
    print("Thrift server started successfully:")
    # print("Installing pyhive===>")
    # subprocess.run("pip install pyhive", shell=True, check=True)
    print("Testing connection to hive metastore `default` database using thrift:")
    create_cloudq_staging_schema()
    main(sys.argv)
