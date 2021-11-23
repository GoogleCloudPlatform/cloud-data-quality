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
import os
import shutil
import tempfile

import pytest
import yaml

from clouddq import lib
from clouddq.classes.dq_config_type import DqConfigType


logger = logging.getLogger(__name__)

class TestLib:

    def test_load_configs_identical(self, temp_configs_dir):
        # Load a config directory containing two copies of the same config
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_lib_1")
            config_path = Path(temp_configs_dir)

            temp_dir.mkdir(parents=True)

            assert os.path.isfile(config_path / 'entities' / 'test-data.yml')
            shutil.copy(config_path / 'entities' / 'test-data.yml', temp_dir / 'test-data1.yml')
            shutil.copy(config_path / 'entities' / 'test-data.yml', temp_dir / 'test-data2.yml')

            loaded_config = lib.load_configs(temp_dir, DqConfigType.ENTITIES)

            assert list(loaded_config.keys()) == ['TEST_TABLE']
        finally:
            shutil.rmtree(temp_dir)

    def test_load_configs_different(self, temp_configs_dir):
        # Load a config directory containing two different configs defining the same key
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_lib_2")
            config_path = Path(temp_configs_dir)

            temp_dir.mkdir(parents=True)

            assert os.path.isfile(config_path / 'entities' / 'test-data.yml')
            shutil.copy(config_path / 'entities' / 'test-data.yml', temp_dir / 'test-data1.yml')

            # Load the config, modify it, and save it to a different file
            with open(config_path / 'entities' / 'test-data.yml') as f:
                testconfig = yaml.safe_load(f)

            assert testconfig['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] == 'STRING'
            testconfig['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] = 'INT64'

            with open(temp_dir / 'test-data2.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            # Check that I wrote that correctly
            with open(temp_dir / 'test-data1.yml') as f:
                testconfig1 = yaml.safe_load(f)
                assert testconfig1['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] == 'STRING'
            with open(temp_dir / 'test-data2.yml') as f:
                testconfig2 = yaml.safe_load(f)
                assert testconfig2['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] == 'INT64'

            # This is the actual test:
            with pytest.raises(ValueError):
                lib.load_configs(temp_dir, DqConfigType.ENTITIES)

        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
