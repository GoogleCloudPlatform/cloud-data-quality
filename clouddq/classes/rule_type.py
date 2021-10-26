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

"""todo: add classes docstring."""
from __future__ import annotations

import re

from enum import Enum
from enum import unique
from string import Template

from clouddq.utils import assert_not_none_or_empty


FORBIDDEN_SQL = re.compile(r"[;\-#|\\/]")
NOT_NULL_SQL = Template("$column IS NOT NULL")
REGEX_SQL = Template("REGEXP_CONTAINS( CAST( $column  AS STRING), '$pattern' )")
NOT_BLANK_SQL = Template("TRIM($column) != ''")


def check_for_invalid_sql(rule_type: RuleType, sql_string: str) -> None:
    if FORBIDDEN_SQL.search(sql_string):
        raise ValueError(
            f"RuleType: {rule_type.name} contains "
            f"invalid characters.\n"
            f"Current value: {sql_string}"
        )


def to_sql_custom_sql_expr(params: dict) -> Template:
    custom_sql_expr = params.get("custom_sql_expr", "")
    assert_not_none_or_empty(
        custom_sql_expr,
        f"RuleType: {RuleType.CUSTOM_SQL_EXPR} must define "
        f"'custom_sql_expr' in 'params'.\n"
        f"Current value: {custom_sql_expr}",
    )
    check_for_invalid_sql(RuleType.CUSTOM_SQL_EXPR, custom_sql_expr)
    return Template(custom_sql_expr)


def to_sql_custom_sql_statement(params: dict) -> Template:
    custom_sql_statement = params.get("custom_sql_statement", "")
    assert_not_none_or_empty(
        custom_sql_statement,
        f"RuleType: {RuleType.CUSTOM_SQL_STATEMENT} must define "
        f"'custom_sql_statement' in 'params'.\n"
        f"Current value: {custom_sql_statement}",
    )
    custom_sql_arguments = params.get("custom_sql_arguments", "")
    if custom_sql_arguments:
        for argument in custom_sql_arguments:
            if f"${argument}" not in custom_sql_statement:
                raise ValueError(
                    f"RuleType: {RuleType.CUSTOM_SQL_STATEMENT} "
                    f"custom_sql_arguments '${argument}' not found in "
                    f"custom_sql_statement:\n {custom_sql_statement}"
                )
            if not params.get("rule_binding_arguments", {}).get(argument, None):
                raise ValueError(
                    f"RuleType: {RuleType.CUSTOM_SQL_STATEMENT} "
                    f"custom_sql_arguments '{argument}' not found in "
                    f"input parameters:\n {params}"
                )
    custom_sql_statement = Template(custom_sql_statement).safe_substitute(
        params.get("rule_binding_arguments")
    )
    check_for_invalid_sql(RuleType.CUSTOM_SQL_STATEMENT, custom_sql_statement)
    return Template(custom_sql_statement)


def to_sql_regex(params: dict) -> Template:
    pattern = params.get("pattern", "")
    assert_not_none_or_empty(
        pattern,
        f"RuleType: {RuleType.REGEX} must define "
        f"'pattern' in 'params'.\n"
        f"Current value: {pattern}",
    )
    try:
        re.compile(pattern)
    except re.error:
        raise ValueError(
            f"RuleType: {RuleType.REGEX} must define "
            f"valid regex 'pattern' in 'params'.\n"
            f"Current value: {pattern}"
        )
    check_for_invalid_sql(RuleType.REGEX, pattern)
    # escape dollar signs in regex
    # to avoid conflict with python string Template
    pattern = pattern.replace("$", "$$")
    return Template(REGEX_SQL.safe_substitute(pattern=pattern))


@unique
class RuleType(str, Enum):
    """ """

    CUSTOM_SQL_STATEMENT = "CUSTOM_SQL_STATEMENT"
    CUSTOM_SQL_EXPR = "CUSTOM_SQL_EXPR"
    NOT_NULL = "NOT_NULL"
    REGEX = "REGEX"
    NOT_BLANK = "NOT_BLANK"

    def to_sql(self: RuleType, params: dict | None) -> Template:
        if self == RuleType.CUSTOM_SQL_EXPR:
            return to_sql_custom_sql_expr(params)
        elif self == RuleType.CUSTOM_SQL_STATEMENT:
            return to_sql_custom_sql_statement(params)
        elif self == RuleType.NOT_NULL:
            return NOT_NULL_SQL
        elif self == RuleType.REGEX:
            return to_sql_regex(params)
        elif self == RuleType.NOT_BLANK:
            return NOT_BLANK_SQL
        else:
            raise NotImplementedError(f"Rule Type: {self} not implemented.")
