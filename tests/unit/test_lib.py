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

            assert loaded_config is not None
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

    def test_load_configs_dimensions(self, temp_configs_dir):
        try:
            temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_lib_load_configs_dims")
            config_path = Path(temp_configs_dir)

            temp_dir.mkdir(parents=True)

            rule_original = config_path / 'rules' / 'base-rules.yml'

            assert os.path.isfile(rule_original)
            shutil.copy(rule_original, temp_dir / 'rule.yml')

            # TEST 1: check that it loads ok
            rules = lib.load_configs(temp_dir, DqConfigType.RULES)
            dims = lib.load_configs(temp_dir, DqConfigType.RULE_DIMENSIONS)

            assert len(rules) == 4
            assert dims == {}
            os.remove(temp_dir / 'rule.yml')

            # TEST 2: add rule_dimensions to the file and load again
            with open(rule_original) as f:
                testconfig = yaml.safe_load(f)

            dims_ref = ['completeness', 'accuracy', 'conformity']
            testconfig[DqConfigType.RULE_DIMENSIONS.value] = dims_ref

            with open(temp_dir / 'rule.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            rules = lib.load_configs(temp_dir, DqConfigType.RULES)
            dims = lib.load_configs(temp_dir, DqConfigType.RULE_DIMENSIONS)

            assert len(rules) == 4, "test 2"
            assert sorted(dims) == sorted(dims_ref), "test 2"
            os.remove(temp_dir / 'rule.yml')

            # TEST 2b: add rule_dimensions to two files and load them
            with open(rule_original) as f:
                testconfig = yaml.safe_load(f)

            dims_ref = ['completeness', 'accuracy', 'conformity']
            testconfig[DqConfigType.RULE_DIMENSIONS.value] = dims_ref

            with open(temp_dir / 'rule1.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)
            with open(temp_dir / 'rule2.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            rules = lib.load_configs(temp_dir, DqConfigType.RULES)
            dims = lib.load_configs(temp_dir, DqConfigType.RULE_DIMENSIONS)

            assert len(rules) == 4, "test 2b"
            assert sorted(dims) == sorted(dims_ref), "test 2b"
            os.remove(temp_dir / 'rule1.yml')
            os.remove(temp_dir / 'rule2.yml')

            # TEST 3: add rule_dimensions to the file, and add some VALID dimensions to rules, and load again
            with open(rule_original) as f:
                testconfig = yaml.safe_load(f)

            testconfig[DqConfigType.RULE_DIMENSIONS.value] = dims_ref
            # add the first dimension to the first rule and the second dimension to the second one
            rule_ids = list(testconfig[DqConfigType.RULES.value])
            testconfig[DqConfigType.RULES.value][rule_ids[0]]['dimension'] = dims_ref[0]
            testconfig[DqConfigType.RULES.value][rule_ids[1]]['dimension'] = dims_ref[1]

            with open(temp_dir / 'rule.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            rules = lib.load_configs(temp_dir, DqConfigType.RULES)
            dims = lib.load_configs(temp_dir, DqConfigType.RULE_DIMENSIONS)

            assert len(rules) == 4, "test 3"
            assert rules[rule_ids[0]]['dimension'] == dims_ref[0], "test 3"
            assert rules[rule_ids[1]]['dimension'] == dims_ref[1], "test 3"
            assert 'dimension' not in rules[rule_ids[2]], "test 3"
            os.remove(temp_dir / 'rule.yml')

            # TEST 4: add an invalid rule dimension to the file
            with open(rule_original) as f:
                testconfig = yaml.safe_load(f)

            testconfig[DqConfigType.RULE_DIMENSIONS.value] = dims_ref
            # add the first dimension to the first rule and the second dimension to the second one
            rule_ids = list(testconfig[DqConfigType.RULES.value])
            testconfig[DqConfigType.RULES.value][rule_ids[0]]['dimension'] = 'bogus'

            with open(temp_dir / 'rule.yml', 'w') as f:
                yaml.safe_dump(testconfig, f)

            rules = lib.load_configs(temp_dir, DqConfigType.RULES)
            dims = lib.load_configs(temp_dir, DqConfigType.RULE_DIMENSIONS)
            os.remove(temp_dir / 'rule.yml')

        finally:
            shutil.rmtree(temp_dir)

    def test_prepare_configs_cache(self, temp_configs_dir):
        config_path = Path(temp_configs_dir)

        temp_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_lib", "test_prepare_configs_cache")

        try:
            temp_dir.mkdir(parents=True)
            shutil.copytree(config_path, temp_dir / 'configs')

            base_rules = temp_dir / 'configs' / 'rules' / 'base-rules.yml'
            assert os.path.isfile(base_rules)
            with (temp_dir / 'configs' / 'rules' / 'base-rules.yml').open() as f:
                rule_config = yaml.safe_load(f)

            # Add dimension "correctness" to all rules

            for id in rule_config['rules']:
                rule_config['rules'][id]['dimension'] = 'correctness'

            # Rewrite the file
            os.remove(base_rules)
            with open(base_rules, 'w') as f:
                yaml.safe_dump(rule_config, f)

            #  Expect to raise a ValueError because no rule_dimensions are defined:
            with pytest.raises(ValueError):
                lib.prepare_configs_cache(temp_dir)

            # Add the rule dimensions and try again
            rule_config['rule_dimensions'] = ['correctness', 'conformity', 'completeness']
            os.remove(base_rules)
            with open(base_rules, 'w') as f:
                yaml.safe_dump(rule_config, f)

            lib.prepare_configs_cache(temp_dir)

        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
