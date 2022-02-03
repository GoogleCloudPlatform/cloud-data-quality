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
import sys

import click.testing
import pytest

from clouddq.main import main
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

@pytest.mark.dataplex
class TestDataplexPerformanceIntegration:

    @pytest.fixture(scope="session")
    def runner(self):
        return click.testing.CliRunner()

    def test_cli_16_rb_10_rules(
        self,
        runner,
        tmp_path,
        gcp_project_id,
        gcp_dataplex_region,
        gcp_dataplex_lake_name,
        gcp_dataplex_zone_id,
        gcp_bq_dataset,
        target_bq_result_dataset_name,
        target_bq_result_table_name):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_16_rb_10_rules")
            temp_dir.mkdir(parents=True)
            source_configs = Path("tests").joinpath("resources", "configs_16_rb_10_rules.yml").absolute()
            # Prepare entity_uri configs
            configs_16_rb_10_rules = temp_dir.joinpath("configs_16_rb_10_rules.yml").absolute()
            target_table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
            with open(source_configs) as source_file:
                lines = source_file.read()
            with open(configs_16_rb_10_rules, "w") as target_file:
                lines = lines.replace("<my-gcp-dataplex-lake-id>", gcp_dataplex_lake_name)
                lines = lines.replace("<my-gcp-dataplex-region-id>", gcp_dataplex_region)
                lines = lines.replace("<my-gcp-project-id>", gcp_project_id)
                lines = lines.replace("<my-gcp-dataplex-zone-id>", gcp_dataplex_zone_id)
                target_file.write(lines)
            with working_directory(temp_dir):
                args = [
                    "ALL",
                    f"{configs_16_rb_10_rules}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--target_bigquery_summary_table={target_table}",
                    "--enable_experimental_dataplex_gcs_validation",
                    "--enable_experimental_bigquery_entity_uris",
                    "--debug",
                    ]
                logger.info(f"Args: {' '.join(args)}")
                result = runner.invoke(main, args)
                logger.info(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', '1'] + sys.argv[1:]))
