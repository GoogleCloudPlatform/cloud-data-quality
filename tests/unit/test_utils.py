# Copyright 2022 Google LLC
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

import pytest

from clouddq import utils


logger = logging.getLogger(__name__)

class TestUtils:

    def test_get_keys_from_dict_and_assert_oneof(self):
        kwargs = {'a': 1, 'b': 2, 'c': 3}
        keys = ['a']
        value = utils.get_keys_from_dict_and_assert_oneof('exactly_one', kwargs=kwargs, keys=keys)
        assert value == {'a': 1}
        with pytest.raises(ValueError):
            utils.get_keys_from_dict_and_assert_oneof('none', kwargs=kwargs, keys=[])
        with pytest.raises(ValueError):
            utils.get_keys_from_dict_and_assert_oneof('two', kwargs=kwargs, keys=['a', 'b'])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
