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

import logging
import os
from pathlib import Path

from google.api_core.exceptions import BadRequest
import pytest

from clouddq import main
from clouddq import utils


@pytest.mark.e2e
class TestDbtBigQuery:
    """ """

    @pytest.fixture
    def test_rule_binding_id(self):
        return "T2_DQ_1_EMAIL"

    @pytest.fixture
    def test_rule_binding_path(self):
        return Path("tests/resources/configs/rule_bindings/team-2-rule-bindings.yml")

    def test_all_configs_files_present(self):
        """ """
        assert os.path.basename(os.getcwd()) != "test"
        files_dir = os.listdir()
        assert "profiles.yml" in files_dir
        assert "dbt_project.yml" in files_dir

    def test_dbt_debug(self):
        """ """
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            logging.info(
                f"GOOGLE_APPLICATION_CREDENTIALS: "
                f"{os.environ['GOOGLE_APPLICATION_CREDENTIALS']}"
            )
        else:
            logging.error(
                "Environment variable GOOGLE_APPLICATION_CREDENTIALS not found."
            )
        if "DBT_GOOGLE_BIGQUERY_KEYFILE" in os.environ:
            logging.info(
                f"DBT_GOOGLE_BIGQUERY_KEYFILE: "
                f"{os.environ['DBT_GOOGLE_BIGQUERY_KEYFILE']}"
            )
        else:
            logging.error("Environment variable DBT_GOOGLE_BIGQUERY_KEYFILE not found.")
        # checks that dbt is running in the project root directory
        # with all required dbt configs defined.
        # note that pytest defaults to starting the tests from /test
        # instead of from the directory root.
        utils.run_dbt(
            debug=True,
            dbt_profile_dir=Path.cwd(),
            configs={},
            environment="test",
            dry_run=True,
        )
        utils.run_dbt(
            debug=True, dbt_profile_dir=Path.cwd(), configs={}, environment="test"
        )

    def test_run_dbt_single_rule_binding_tests_failure(
        self, test_rule_binding_id, test_rule_binding_path
    ):
        with pytest.raises(BadRequest):
            main.load_all_rule_bindings_and_run_dbt(
                dbt_profile_dir=Path.cwd(),
                dbt_path=Path("dbt"),
                target_rule_binding_ids=[test_rule_binding_id],
                rule_binding_config_path=test_rule_binding_path,
                environment="test",
                metadata={"brand": "test"},
                debug=True,
            )

    def test_run_dbt_single_rule_binding_dry_run(
        self, test_rule_binding_id, test_rule_binding_path
    ):
        main.load_all_rule_bindings_and_run_dbt(
            dbt_profile_dir=Path.cwd(),
            dbt_path=Path("dbt"),
            target_rule_binding_ids=[test_rule_binding_id],
            rule_binding_config_path=test_rule_binding_path,
            environment="dev",
            metadata={"brand": "test"},
            debug=True,
            dry_run=True,
        )

    def test_run_dbt_single_rule_binding_custom_configs_path(
        self, test_rule_binding_id, test_rule_binding_path
    ):
        main.load_all_rule_bindings_and_run_dbt(
            dbt_profile_dir=Path.cwd(),
            target_rule_binding_ids=[test_rule_binding_id],
            rule_binding_config_path=test_rule_binding_path,
            configs_path=Path("configs"),
            dbt_path=Path("dbt"),
            environment="dev",
            metadata={"brand": "test"},
            progress_watermark=False,
        )
        # todo: check bq table results

    @pytest.mark.parametrize(
        "rule_binding_config_path",
        [
            "tests/resources/configs/rule_bindings/team-2-rule-bindings.yml",
            "tests/resources/configs/rule_bindings",
        ],
    )
    def test_load_all_rule_bindings_and_run_dbt(self, rule_binding_config_path):
        main.load_all_rule_bindings_and_run_dbt(
            dbt_profile_dir=Path.cwd(),
            dbt_path=Path("dbt"),
            rule_binding_config_path=rule_binding_config_path,
            environment="dev",
            metadata={"brand": "test"},
            debug=True,
            progress_watermark=False,
        )
        # todo: check bq table results
