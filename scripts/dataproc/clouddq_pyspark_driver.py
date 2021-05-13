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

"""PySpark Driver for launching CloudDQ as a Spark Job

This is designed to be launched using a companion script such 
as `scripts/dataproc/submit-dataproc-job.sh`.
"""


import sys
import subprocess
import os
import hashlib

from zipfile import ZipFile


def verify_executable(fname, expected_hexdigest):
    hash_sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    if not hash_sha256.hexdigest() == expected_hexdigest:
       raise ValueError(f"Cannot verify executable {fname}.")

def main(args):
   print(f"Run configs: {args}")
   with open(f'{args[1]}.hashsum', 'r') as f:
      expected_hexdigest = f.read().replace('\n','').replace('\t','')
      verify_executable(args[1], expected_hexdigest)
   cmd = f"python3 {' '.join(args[1:])}"
   print(f"Executing commands:\n {cmd}")
   subprocess.run(cmd, shell=True)

if __name__ == "__main__":
   with ZipFile('clouddq-configs.zip', 'r') as zipObject:
      zipObject.extractall()
   print(os.listdir())
   main(sys.argv)
