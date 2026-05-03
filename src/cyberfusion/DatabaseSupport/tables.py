"""Classes for interaction with tables."""

from typing import TYPE_CHECKING, List

from sqlalchemy.schema import Index, Table as SQLAlchemyTable
from sqlalchemy.sql import text

from cyberfusion.DatabaseSupport.exceptions import IndexExistsError, InvalidInputError

if TYPE_CHECKING:  # pragma: no cover
    from cyberfusion.DatabaseSupport.databases import Database

import re

from cyberfusion.DatabaseSupport.exceptions import ServerNotSupportedError
from cyberfusion.DatabaseSupport.queries import Query
from cyberfusion.DatabaseSupport.utilities import object_exists


class Table:
    """Abstract representation of table."""

    CHAR_NAME_DATABASE_WILDCARD = "*"

    def __init__(
        self,
        *,
        database: "Database",
        name: str,
    ) -> None:
        """Raise exception if server software not supported, set attributes, and call functions to handle table."""
        self.database = database
        self.name = name

    @property
    def name(self) -> str:
        """Get name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set name."""

        # Variable must be alphanumeric to prevent SQL injection; DDL statements
        # do not support quoted identifiers

        if not re.match(r"^[a-zA-Z0-9-_]+$", value):
            raise InvalidInputError(value)

        self._name = value

    @property
    def exists(self) -> bool:
        """Get table exists locally."""
        for table in self.database.tables:
            if table.name != self.name:
                continue

            return True

        return False

    @property
    def checksum(self) -> int:
        """Get checksum."""
        if (
            self.database.server_software_name
            != self.database.support.MARIADB_SERVER_SOFTWARE_NAME
        ):
            raise ServerNotSupportedError

        return Query(
            engine=self.database.support.engines.engines[
                self.database.support.engines.MYSQL_ENGINE_NAME
            ],
            query=text(
                f"CHECKSUM TABLE `{self.database.name}`.`{self.name}`;"
            ).bindparams(),
        ).result[0][1]

    @property
    def _table_name_with_schema_name(self) -> str:
        """Get table name with schema name."""
        return self.database.name + "." + self.name

    @property
    def reflection(self) -> SQLAlchemyTable:
        """Get reflected table from database."""
        return self.database.metadata.tables[self._table_name_with_schema_name]

    @object_exists
    def create_index(self, *, name: str, columns: List[str]) -> None:
        """Create index on table."""
        reflection = self.reflection

        for existing_index in reflection.indexes:
            if existing_index.name == name:
                raise IndexExistsError(name)

        index = Index(
            name,
            *[reflection.c[column] for column in columns],
        )

        index.create(bind=self.database.database_engine)

    @object_exists
    def get_indexes_by_column(self, *, column: str) -> List[Index]:
        """Get indexes that contain given column."""
        indexes = []

        for index in self.reflection.indexes:
            if column in [c.name for c in index.columns]:
                indexes.append(index)

        return indexes

    @object_exists
    def drop(self) -> bool:
        """Drop table."""
        self.reflection.drop(bind=self.database.database_engine)

        return True
