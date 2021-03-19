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

load("@bazel_skylib//lib:shell.bzl", "shell")

def _lint_impl(ctx):
    bash_file = ctx.actions.declare_file(ctx.label.name + ".bash")
    substitutions = {
        "@@MODE@@": shell.quote(ctx.attr.mode),
        "@@BLACK_PATH@@": shell.quote(ctx.executable._black.short_path),
        "@@BUILDIFIER_PATH@@": shell.quote(ctx.executable._buildifier.short_path),
        "@@PYUPGRADE_PATH@@": shell.quote(ctx.executable._pyupgrade.short_path),
        "@@ISORT_PATH@@": shell.quote(ctx.executable._isort.short_path),
        "@@FLAKE8_PATH@@": shell.quote(ctx.executable._flake8.short_path),
    }
    ctx.actions.expand_template(
        template = ctx.file._runner,
        output = bash_file,
        substitutions = substitutions,
        is_executable = True,
    )
    runfiles = ctx.runfiles(files = [
        ctx.executable._buildifier,
        ctx.executable._black,
        ctx.executable._pyupgrade,
        ctx.executable._isort,
        ctx.executable._flake8,
    ])
    return [DefaultInfo(
        files = depset([bash_file]),
        runfiles = runfiles,
        executable = bash_file,
    )]

_lint = rule(
    implementation = _lint_impl,
    attrs = {
        "mode": attr.string(
            default = "lint",
            doc = "Formatting mode",
            values = ["lint", "check"],
        ),
        "_black": attr.label(
            default = "//tools/lint:black",
            cfg = "host",
            executable = True,
        ),
        "_buildifier": attr.label(
            default = "@com_github_bazelbuild_buildtools//buildifier",
            cfg = "host",
            executable = True,
        ),
        "_pyupgrade": attr.label(
            default = "//tools/lint:pyupgrade",
            cfg = "host",
            executable = True,
        ),
        "_isort": attr.label(
            default = "//tools/lint:isort",
            cfg = "host",
            executable = True,
        ),
        "_flake8": attr.label(
            default = "//tools/lint:flake8",
            cfg = "host",
            executable = True,
        ),
        "_runner": attr.label(
            default = "//tools/lint:lint.tpl",
            allow_single_file = True,
        ),
    },
    executable = True,
)

def lint(**kwargs):
    tags = kwargs.get("tags", [])
    if "manual" not in tags:
        tags.append("manual")
        kwargs["tags"] = tags
    _lint(**kwargs)
