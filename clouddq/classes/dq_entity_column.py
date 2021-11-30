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

from dataclasses import dataclass
from enum import Enum
from enum import unique

from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import get_from_dict_and_assert


@dataclass
class DqEntityColumn:
    """ """

    column_id: str
    column_name: str
    column_type: str
    column_description: str | None
    entity_source_database: str

    def get_column_type_value(self: DqEntityColumn) -> str:
        return DatabaseType(self.entity_source_database).get_column_type(
            self.column_type
        )

    @classmethod
    def from_dict(
        cls: DqEntityColumn,
        entity_column_id: str,
        kwargs: dict,
        entity_source_database: str,
    ) -> DqEntityColumn:
        """

        Args:
          cls: DqEntityColumn:
          entity_column_id: str:
          kwargs: typing.Dict:
          entity_source_database: str

        Returns:

        """
        source_database: DatabaseType = DatabaseType(entity_source_database)
        column_name = get_from_dict_and_assert(
            config_id=entity_column_id, kwargs=kwargs, key="name"
        )
        column_type = get_from_dict_and_assert(
            config_id=entity_column_id,
            kwargs=kwargs,
            key="data_type",
            assertion=lambda x: source_database.get_column_type(x) is not None,
            error_msg=f"Invalid Column Type for Database {source_database}.",
        )
        column_description = kwargs.get("description", None)
        return DqEntityColumn(
            column_id=entity_column_id,
            column_name=column_name,
            column_type=column_type,
            column_description=column_description,
            entity_source_database=source_database,
        )

    def to_dict(self: DqEntityColumn) -> dict:
        """

        Args:
          self: DqEntityColumn:

        Returns:

        """
        output = {
            "name": self.column_name,
            "data_type": self.column_type,
        }
        if self.column_description:
            output["description"] = self.column_description
        return dict({f"{self.column_id}": output})

    def dict_values(self: DqEntityColumn) -> dict:
        """

        Args:
          self: DqEntityColumn:

        Returns:

        """

        return dict(self.to_dict().get(self.column_id))


@unique
class DatabaseType(str, Enum):
    """ """

    BIGQUERY = "BIGQUERY"
    DATAPLEX_BQ_EXTERNAL_TABLE = "DATAPLEX_BQ_EXTERNAL_TABLE"

    def get_column_type(
        self: DqEntityColumn, column_type: DatabaseColumnType | str
    ) -> str:
        if type(column_type) != DatabaseColumnType:
            column_type = DatabaseColumnType(column_type)
        if self == DatabaseType.BIGQUERY:
            database_column_type = BIGQUERY_COLUMN_TYPES_MAPPING.get(column_type, None)
            assert_not_none_or_empty(
                database_column_type,
                f"Database: {self} does not have type {column_type} "
                f"defined in column type mapping."
                f"Current mapping: {BIGQUERY_COLUMN_TYPES_MAPPING}",
            )
            return database_column_type
        if self == DatabaseType.DATAPLEX_BQ_EXTERNAL_TABLE:
            raise NotImplementedError(
                f"Database: {self} does not yet have column type mapping."
            )
        else:
            raise NotImplementedError(
                f"Database: {self} does not yet have column type mapping."
            )


@unique
class DatabaseColumnType(str, Enum):
    """ """

    STRING = "STRING"
    CHAR = "CHAR"
    NCHAR = "NCHAR"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    TEXT = "TEXT"
    INT = "INT"
    INTEGER = "INTEGER"
    INT64 = "INT64"
    SMALLINT = "SMALLINT"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    FLOAT64 = "FLOAT64"
    REAL = "REAL"
    DOUBLE = "DOUBLE"
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"
    BOOL = "BOOL"
    BOOLEAN = "BOOLEAN"
    TINYINT = "TINYINT"
    BIT = "BIT"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    BINARY = "BINARY"
    BYTES = "BYTES"
    INTERVAL = "INTERVAL"
    GEOGRAPHY = "GEOGRAPHY"


BIGQUERY_COLUMN_TYPES_MAPPING: dict = {
    DatabaseColumnType.STRING: "STRING",
    DatabaseColumnType.CHAR: "STRING",
    DatabaseColumnType.NCHAR: "STRING",
    DatabaseColumnType.VARCHAR: "STRING",
    DatabaseColumnType.NVARCHAR: "STRING",
    DatabaseColumnType.TEXT: "STRING",
    DatabaseColumnType.INT: "INT64",
    DatabaseColumnType.INTEGER: "INT64",
    DatabaseColumnType.INT64: "INT64",
    DatabaseColumnType.SMALLINT: "INT64",
    DatabaseColumnType.BIGINT: "INT64",
    DatabaseColumnType.FLOAT: "FLOAT64",
    DatabaseColumnType.FLOAT64: "FLOAT64",
    DatabaseColumnType.REAL: "FLOAT64",
    DatabaseColumnType.DOUBLE: "FLOAT64",
    DatabaseColumnType.NUMERIC: "NUMERIC",
    DatabaseColumnType.DECIMAL: "NUMERIC",
    DatabaseColumnType.BOOL: "BOOL",
    DatabaseColumnType.BOOLEAN: "BOOL",
    DatabaseColumnType.TINYINT: "BOOL",
    DatabaseColumnType.BIT: "BOOL",
    DatabaseColumnType.DATETIME: "DATETIME",
    DatabaseColumnType.TIMESTAMP: "TIMESTAMP",
    DatabaseColumnType.DATE: "DATE",
    DatabaseColumnType.TIME: "TIME",
    DatabaseColumnType.ARRAY: "ARRAY",
    DatabaseColumnType.STRUCT: "STRUCT",
    DatabaseColumnType.BINARY: "BYTES",
    DatabaseColumnType.BYTES: "BYTES",
    DatabaseColumnType.INTERVAL: None,  # BQ has no INTERVAL type
    DatabaseColumnType.GEOGRAPHY: "GEOGRAPHY",
}
