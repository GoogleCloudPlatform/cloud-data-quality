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

import hashlib
import os
import subprocess
import sys

from pathlib import Path
from pprint import pprint
from zipfile import ZipFile


def verify_executable(fname, expected_hexdigest):
    hash_sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    if not hash_sha256.hexdigest() == expected_hexdigest:
        raise ValueError(f"Cannot verify executable {fname}.")


def main(args):
    with open(f"{args[1]}.hashsum") as f:
        expected_hexdigest = f.read().replace("\n", "").replace("\t", "")
        verify_executable(args[1], expected_hexdigest)
    args[3] = Path("configs").absolute()
    cmd = f"python3 {' '.join(args[1:])}"
    print(f"Executing commands:\n {cmd}")
    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    print("PySpark working directory:")
    pprint(Path().absolute())
    print("PySpark directory content:")
    pprint(os.listdir())
    print("Input PySpark arguments:")
    pprint(sys.argv)
    input_configs = sys.argv[3]
    print("User-specified CloudDQ YAML configs: {input_configs}")
    configs_path = Path("configs")
    if not configs_path.is_dir():
        print(f"Creating configs directory at: `{configs_path.absolute()}`")
        configs_path.mkdir()
    for filename in os.listdir():
        file = Path(filename)
        # checking if it is a file
        if file.is_file():
            if file.suffix == ".zip" and file.name != "clouddq-executable.zip":
                print(
                    f"Extracting file {file} to configs directory `{configs_path}`..."
                )
                with ZipFile(file, "r") as zipObject:
                    zipObject.extractall(configs_path)
            elif file.suffix == ".yml" or file.suffix == ".yaml":
                print(
                    f"Copying YAML file {file} to configs directory `{configs_path}`..."
                )
                configs_path.joinpath(file.name).write_text(file.open().read())
        # else if it's a directory,
        # look for yaml/yml files in the path
        # and copy them to the `configs` directory
        elif file.is_dir():
            for yaml_file in file.glob("**/*.yaml"):
                configs_path.joinpath(yaml_file).write_text(yaml_file.open().read())
            for yaml_file in file.glob("**/*.yml"):
                configs_path.joinpath(yaml_file).write_text(yaml_file.open().read())
    print("Configs directory contents is:")
    pprint(configs_path.glob("**/*"))
    main(sys.argv)
