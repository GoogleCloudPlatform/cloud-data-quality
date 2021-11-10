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

import os
import subprocess
import sys

from pathlib import Path
from pprint import pprint

if __name__ == "__main__":
    print("OS Runtime Details:")
    subprocess.run("cat /etc/*-release", shell=True, check=True)
    print("Python Version:")
    print(sys.version_info)
    print("Python Versions Available:")
    subprocess.run("which python3", shell=True, check=True)
    subprocess.run("ls -la /usr/bin/python*", shell=True, check=True)
    subprocess.run("ls -la /opt/dataproc/opt/conda/default/lib/python*", shell=True, check=True)
    subprocess.run("ls -la /opt/dataproc/opt/conda/default/lib/python3.9/bin/", shell=True, check=True)
    print("PySpark working directory:")
    pprint(Path().absolute())
    print("PySpark directory content:")
    pprint(os.listdir())
    print("Input PySpark arguments:")
    pprint(sys.argv)