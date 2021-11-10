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

workspace(name = "clouddq")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# set up buildifier
# buildifier is written in Go and hence needs rules_go to be built.
# See https://github.com/bazelbuild/rules_go for the up to date setup instructions.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "d1ffd055969c8f8d431e2d439813e42326961d0942bdf734d2c95dc30c369566",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.24.5/rules_go-v0.24.5.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.24.5/rules_go-v0.24.5.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

http_archive(
    name = "bazel_gazelle",
    sha256 = "b85f48fa105c4403326e9525ad2b2cc437babaa6e15a3fc0b1dbab0ab064bc7c",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.22.2/bazel-gazelle-v0.22.2.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.22.2/bazel-gazelle-v0.22.2.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

# If you use WORKSPACE.bazel, use the following line instead of the bare gazelle_dependencies():
# gazelle_dependencies(go_repository_default_config = "@//:WORKSPACE.bazel")
gazelle_dependencies()

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-master",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/master.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-master",
    url = "https://github.com/bazelbuild/buildtools/archive/master.zip",
)

# http_archive(
#     name = "dpu_rules_pyenv",
#     sha256 = "d057168a757efa74e6345edd4776a1c0f38134c2d48eea4f3ef4783e1ea2cb0f",
#     strip_prefix = "rules_pyenv-0.1.4",
#     urls = ["https://github.com/digital-plumbers-union/rules_pyenv/archive/v0.1.4.tar.gz"],
# )

rules_python_version = "ef4d735216a3782b7c33543d82b891fe3a86e3f3"

http_archive(
    name = "rules_python",
    sha256 = "031619e49763c8c393ea57f1964a41abcbe76ac4ea92e4c646ddcc45df8bed97",
    strip_prefix = "rules_python-{}".format(rules_python_version),
    url = "https://github.com/bazelbuild/rules_python/archive/{}.zip".format(rules_python_version),
)

load("@rules_python//python:pip.bzl", "pip_install")

# Create a central repo that knows about the dependencies needed from
# requirements_lock.txt.
pip_install(
    name = "py_deps",
    python_interpreter = "/usr/bin/python3",
    quiet = False,
    requirements = "//:requirements.txt",
)

# load("@dpu_rules_pyenv//pyenv:defs.bzl", "pyenv_install")

# pyenv_install(
#     py2 = "2.7.16",
#     py3 = "3.8.6",
# )

register_toolchains("//:my_toolchain")