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

from pathlib import Path

import logging
import shutil

import pytest

from clouddq.runners.dbt.dbt_runner import DbtRunner
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

GOOGLE_LICENSE: str = """
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
"""

class TestDbtRunner:

    def test_num_threads(
        self,
        runner,
        gcp_application_credentials,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_bq_region,
        target_bq_result_dataset_name,
        target_bq_result_table_name,
        tmp_path,
        gcp_impersonation_credentials,
        gcp_sa_key,
        test_resources,
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("cloud_dq_working_dir")
            temp_dir.mkdir(parents=True)

            with working_directory(temp_dir):
                intermediate_table_expiration_hours = 24
                num_threads = 10
                dbt_runner = DbtRunner(
                    dbt_path=None,
                    dbt_profiles_dir=None,
                    environment_target="Dev",
                    gcp_project_id=gcp_project_id,
                    gcp_region_id=gcp_bq_region,
                    gcp_bq_dataset_id=gcp_bq_dataset,
                    gcp_service_account_key_path=gcp_sa_key,
                    gcp_impersonation_credentials=gcp_impersonation_credentials,
                    intermediate_table_expiration_hours=intermediate_table_expiration_hours,
                    num_threads=num_threads,
                )
                profiles_yml_actual = dbt_runner.connection_config.to_dbt_profiles_yml(environment_target="Dev")\
                                                .strip()

                with open(test_resources / "expected_test_profiles.yml") as source_file:
                    profiles_yml_expected = source_file.read().strip()
                    profiles_yml_expected = profiles_yml_expected.replace(GOOGLE_LICENSE.strip(), "").strip()
                    profiles_yml_expected = profiles_yml_expected.replace('<my-gcp-project-id>', gcp_project_id)
                    profiles_yml_expected = profiles_yml_expected.replace('<my-gcp-dataset-id>', gcp_bq_dataset)
                    profiles_yml_expected = profiles_yml_expected.replace('<my-gcp-region-id>', gcp_bq_region)
                    profiles_yml_expected = profiles_yml_expected\
                        .replace('<my-gcp-impersonation-credentials>', gcp_impersonation_credentials)
                    profiles_yml_expected = profiles_yml_expected\
                        .replace('<my-gcp-sa-key>', gcp_sa_key)
                    profiles_yml_expected = profiles_yml_expected.replace("'", "")
                    assert profiles_yml_actual == profiles_yml_expected

        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
