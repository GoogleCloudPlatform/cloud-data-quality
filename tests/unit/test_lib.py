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


from clouddq.main import lib

import os
import logging
import pytest
import tempfile
import shutil
import yaml

from pathlib import Path
from clouddq.classes.dq_config_type import DqConfigType


logger = logging.getLogger(__name__)

class TestLib:


    # TODO Clean up and reuse from inherited code when it becomes available
    @pytest.fixture
    def gcp_project_id(self):
        gcp_project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', None)
        if not gcp_project_id:
            logger.fatal("Required test environment variable GOOGLE_CLOUD_PROJECT cannot be found. Set this to the project_id used for integration testing.")
        return gcp_project_id


    # TODO Clean up and reuse from inherited code when it becomes available
    @pytest.fixture
    def gcp_bq_dataset(self):
        gcp_bq_dataset = os.environ.get('CLOUDDQ_BIGQUERY_DATASET', None)
        if not gcp_bq_dataset:
            logger.fatal("Required test environment variable CLOUDDQ_BIGQUERY_DATASET cannot be found. Set this to the BigQuery dataset used for integration testing.")
        return gcp_bq_dataset


    # TODO Clean up and reuse from inherited code when it becomes available
    @pytest.fixture
    def temp_configs_dir(self, gcp_project_id, gcp_bq_dataset):
        source_configs_path = Path("tests").joinpath("resources","configs")
        temp_clouddq_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "configs")
        if os.path.exists(temp_clouddq_dir):
            shutil.rmtree(temp_clouddq_dir)
        destination = shutil.copytree(source_configs_path, temp_clouddq_dir)
        test_data = Path(destination).joinpath("entities", "test-data.yml")
        with open(test_data) as source_file:
            lines = source_file.read()
        with open(test_data, "w") as source_file:
            lines = lines.replace("<your_gcp_project_id>", gcp_project_id)
            lines = lines.replace("dq_test", gcp_bq_dataset)
            source_file.write(lines)
        yield destination
        shutil.rmtree(destination)


    # Load a config directory containing two copies of the same config
    def test_load_configs_identical(self, temp_configs_dir):
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_lib")
            config_path = Path(temp_configs_dir) 

            os.mkdir(temp_dir)

            assert os.path.isfile(config_path / 'entities' / 'test-data.yml')
            shutil.copy(config_path / 'entities' / 'test-data.yml', temp_dir / 'test-data1.yml')
            shutil.copy(config_path / 'entities' / 'test-data.yml', temp_dir / 'test-data2.yml')

            loaded_config = lib.load_configs(temp_dir, DqConfigType.ENTITIES)

            assert list(loaded_config.keys()) == ['TEST_TABLE']
        finally:
            shutil.rmtree(temp_dir)


    # Load a config directory containing two different configs defining the same key
    def test_load_configs_identical(self, temp_configs_dir):
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_lib")
            config_path = Path(temp_configs_dir) 

            os.mkdir(temp_dir)

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
            with  open(temp_dir / 'test-data1.yml') as f:
                testconfig1 = yaml.safe_load(f)
                assert testconfig1['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] == 'STRING'
            with  open(temp_dir / 'test-data2.yml') as f:
                testconfig2 = yaml.safe_load(f)
                assert testconfig2['entities']['TEST_TABLE']['columns']['ROW_ID']['data_type'] == 'INT64'
                
            # This is the actual test:
            with pytest.raises(ValueError):
                lib.load_configs(temp_dir, DqConfigType.ENTITIES)

        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv']))
