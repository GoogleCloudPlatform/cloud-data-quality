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
gazelle_dependencies(go_repository_default_config = "@//:WORKSPACE.bazel")

gazelle_dependencies()

protobuf_version = "3.19.1"

http_archive(
    name = "com_google_protobuf",
    sha256 = "25f1292d4ea6666f460a2a30038eef121e6c3937ae0f61d610611dfb14b0bd32",
    strip_prefix = "protobuf-{}".format(protobuf_version),
    urls = ["https://github.com/protocolbuffers/protobuf/archive/refs/tags/v{}.zip".format(protobuf_version)],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

buildtools_version = "4.2.3"

http_archive(
    name = "com_github_bazelbuild_buildtools",
    sha256 = "c1d5de8802be326300f7a481c2530cf7bfa911e52f46252c71351f9dd305535d",
    strip_prefix = "buildtools-{}".format(buildtools_version),
    url = "https://github.com/bazelbuild/buildtools/archive/refs/tags/{}.zip".format(buildtools_version),
)

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
    python_interpreter = "python3",
    quiet = False,
    requirements = "//:requirements.txt",
)

register_toolchains("//:my_toolchain")
