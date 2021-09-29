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

import click.testing
import logging
import pytest

from clouddq.main import main

logger = logging.getLogger(__name__)

class TestCli:
    @pytest.fixture
    def runner(self):
        return click.testing.CliRunner()

    def test_cli_no_args_fail(self, runner):
        result = runner.invoke(main)
        logger.info(result.output)
        assert result.exit_code == 2

    def test_cli_help_text(self, runner):
        result = runner.invoke(main, ["--help"])
        logger.info(result.output)
        assert result.exit_code == 0

    def test_cli_missing_dbt_profiles_dir_fail(self, runner):
        args = [
            "ALL", 
            "tests/resources/configs", 
            "--dry_run", 
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        logger.info(result.output)
        assert result.exit_code == 2

    def test_cli_dry_run(self, runner):
        args = [
            "ALL", 
            "tests/resources/configs", 
            "--dbt_profiles_dir=tests/resources/test_dbt_profiles_dir", 
            "--dry_run", 
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        logger.info(result.output)
        assert result.exit_code == 0

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
