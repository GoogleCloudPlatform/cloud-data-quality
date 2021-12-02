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

import typing


def update_config(config_old: typing.Dict, config_new: typing.Dict) -> typing.Dict:

    if not config_old and not config_new:
        return {}
    elif not config_old:
        return config_new.copy()
    elif not config_new:
        return config_old.copy()
    else:
        intersection = config_old.keys() & config_new.keys()

        # The new config defines keys that we have already loaded
        if intersection:
            # Verify that objects pointed to by duplicate keys are identical
            config_old_i = {}
            config_new_i = {}
            for k in intersection:
                config_old_i[k] = config_old[k]
                config_new_i[k] = config_new[k]

            # == on dicts performs deep compare:
            if not config_old_i == config_new_i:
                raise ValueError(
                    f"Detected Duplicated Config ID(s): {intersection} "
                    f"If a config ID is repeated, it must be for an identical "
                    f"configuration."
                )

        updated = config_old.copy()
        updated.update(config_new)
        return updated


def update_config_lists(config_old: list, config_new: list) -> list:

    if not config_old and not config_new:
        return []
    elif not config_old:
        return config_new.copy()
    elif not config_new:
        return config_old.copy()
    else:
        # Both lists contain data. This is only OK if they are identical.
        if not sorted(config_old) == sorted(config_new):
            raise ValueError(
                f"Detected Duplicated Config: {config_new}."
                f"If a config is repeated, it must be identical."
            )
        return config_old.copy()
