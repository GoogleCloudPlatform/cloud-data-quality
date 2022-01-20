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

from pathlib import Path
from pprint import pprint

import os
import subprocess
import sys

# from pyhive import hive

from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from contextlib import closing


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
        print(str(e))
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
    print("Pyhive Version")
    print("pyhive.__version__")
    print("Start thrift server:")
    cmd = f"""export HIVE_SERVER2_THRIFT_PORT=10005;
    export HIVE_SERVER2_THRIFT_BIND_HOST=localhost;
    /usr/lib/spark/sbin/start-thriftserver.sh"""
    print(f"Executing commands:\n {cmd}")
    subprocess.run(cmd, shell=True, check=True)
    print("Thrift server started successfully:")
    print("Testing connection to hive metastore `default` database using thrift:")
    # connection = hive.connect(
    #     host="localhost", port=10005, database='default', user="root"
    # )
    # with closing(connection):
    #     cursor = connection.cursor()
    #     cursor.execute("SHOW TABLES;")
    #     rows = list(cursor.fetchall())
    #     print(f"Connected to hive metastore successfully. We have {len(rows)} tables in default database.")
    # create_cloudq_staging_schema()
