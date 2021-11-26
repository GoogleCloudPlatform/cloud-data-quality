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

import pytest
import logging
import os
import shutil
import tempfile
import yaml

from clouddq.lib import load_metadata_registry_default_configs
from clouddq.classes.metadata_registry_defaults import MetadataRegistryDefaults


logger = logging.getLogger(__name__)

class TestMetadataRegistryDefaults:

    @pytest.fixture(scope="session")
    def dataplex_expected_defaults(self):
        expected_defaults = {
            "dataplex": {
                "projects": "<my-gcp-project-id>",
                "locations": "<my-gcp-dataplex-region-id>",
                "lakes": "<my-gcp-dataplex-lake-id>",
                "zones": "<my-gcp-dataplex-zone-id>",
            }
        }
        return expected_defaults

    def test_load_metadata_registry_default_configs(self, source_configs_path, dataplex_expected_defaults):
        metadata_defaults = load_metadata_registry_default_configs(configs_path=source_configs_path)
        assert 'dataplex' in metadata_defaults.default_configs
        assert metadata_defaults.get_dataplex_registry_defaults('projects') == "<my-gcp-project-id>"
        assert metadata_defaults.get_dataplex_registry_defaults('locations') == "<my-gcp-dataplex-region-id>"
        assert metadata_defaults.get_dataplex_registry_defaults('lakes') == "<my-gcp-dataplex-lake-id>"
        assert metadata_defaults.get_dataplex_registry_defaults('zones') == "<my-gcp-dataplex-zone-id>"
        assert metadata_defaults.get_dataplex_registry_defaults() == dataplex_expected_defaults['dataplex']

    def test_from_dict_metadata_registry_default_configs(self, dataplex_expected_defaults):
        metadata_registry_defaults = MetadataRegistryDefaults.from_dict(dataplex_expected_defaults)
        assert metadata_registry_defaults.to_dict() == dataplex_expected_defaults

    def test_from_dict_metadata_registry_default_configs_unsupported(self):
        input_dict = {
            "bigquery": {
                "projects": "<my-gcp-project-id>",
            }
        }
        with pytest.raises(NotImplementedError):
            MetadataRegistryDefaults.from_dict(input_dict)

    def test_from_dict_metadata_registry_default_configs_unexpected_key(self):
        input_dict = {
            "dataplex": {
                "dataset": "<my-gcp-bq-dataset>",
            }
        }
        with pytest.raises(ValueError):
            MetadataRegistryDefaults.from_dict(input_dict)

    def test_load_undefined_configs_ok(self):
        # Load a config directory containing two copies of the same config
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_metadata_registry_defaults_0")
            temp_dir.mkdir(parents=True)

            loaded_config = load_metadata_registry_default_configs(temp_dir)
            assert type(loaded_config) == MetadataRegistryDefaults

            assert loaded_config.to_dict() == {}
        finally:
            shutil.rmtree(temp_dir)

    def test_load_configs_identical(self, source_configs_path, dataplex_expected_defaults):
        # Load a config directory containing two copies of the same config
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_metadata_registry_defaults_1")
            config_path = Path(source_configs_path)

            temp_dir.mkdir(parents=True)

            assert os.path.isfile(config_path / 'metadata_registry_defaults.yml')
            shutil.copy(config_path / 'metadata_registry_defaults.yml', temp_dir / 'metadata_registry_defaults1.yml')
            shutil.copy(config_path / 'metadata_registry_defaults.yml', temp_dir / 'metadata_registry_defaults2.yml')

            loaded_config = load_metadata_registry_default_configs(temp_dir)

            assert loaded_config.to_dict() == dataplex_expected_defaults
        finally:
            shutil.rmtree(temp_dir)

    def test_load_configs_different(self, source_configs_path, dataplex_expected_defaults):
        # Load a config directory containing two different configs defining the same key
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_metadata_registry_defaults_2")
            config_path = Path(source_configs_path)

            temp_dir.mkdir(parents=True)

            assert os.path.isfile(config_path / 'metadata_registry_defaults.yml')
            shutil.copy(config_path / 'metadata_registry_defaults.yml', temp_dir / 'metadata_registry_defaults1.yml')

            loaded_config = load_metadata_registry_default_configs(temp_dir)

            assert loaded_config.to_dict() == dataplex_expected_defaults

            # Load the config, modify it, and save it to a different file
            with open(config_path / 'metadata_registry_defaults.yml') as f:
                testconfig = yaml.safe_load(f)

            assert testconfig == {'metadata_registry_defaults':dataplex_expected_defaults}
            testconfig['metadata_registry_defaults']['dataplex']['projects'] = '<my-gcp-project-id-2>'

            with open(temp_dir / 'metadata_registry_defaults2.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            with open(temp_dir / 'metadata_registry_defaults2.yml') as f:
                testconfig2 = yaml.safe_load(f)
                assert testconfig2['metadata_registry_defaults']['dataplex']['projects'] == '<my-gcp-project-id-2>'

            # This is the actual test:
            with pytest.raises(ValueError):
                load_metadata_registry_default_configs(temp_dir)

        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
