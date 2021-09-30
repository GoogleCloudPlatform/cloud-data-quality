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

from google.api_core.exceptions import BadRequest
import pytest

from clouddq.classes.dry_run_bigquery import BigQueryDryRunClient


@pytest.mark.e2e
class TestBigQueryDryRunClient:

    def test_failure(self):
        query = "SELECT 1; drop table students;--"
        with pytest.raises(BadRequest):
            BigQueryDryRunClient.check_query(query)

    def test_validate_sql(self):
        query = "SELECT 1 + 2"
        BigQueryDryRunClient.check_query(query)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
