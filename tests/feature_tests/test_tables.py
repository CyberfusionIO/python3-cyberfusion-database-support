from typing import Generator

import pytest
from sqlalchemy import Table as SQLAlchemyTable

from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import (
    InvalidInputError,
    ServerNotSupportedError,
)
from cyberfusion.DatabaseSupport.tables import Table
from cyberfusion.DatabaseSupport.utilities import generate_random_string


@pytest.mark.mariadb
def test_table_create_wrong_name(
    mariadb_database: Generator[Database, None, None],
) -> None:
    NAME = "ha^llo"

    with pytest.raises(InvalidInputError) as e:
        Table(
            database=mariadb_database,
            name=NAME,
        )

    assert e.value.input_ == NAME


@pytest.mark.mariadb
def test_mariadb_table_not_exists(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_table_1: Generator[Table, None, None],
) -> None:
    assert not mariadb_table_1.exists


@pytest.mark.mariadb
def test_mariadb_table_not_exists_by_name(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_table_created_1: Generator[Table, None, None],
    mariadb_table_2: Generator[Table, None, None],
) -> None:
    assert not Table(
        database=mariadb_database_created_1,
        name=generate_random_string(),
    ).exists


@pytest.mark.mariadb
def test_mariadb_table_exists(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    assert mariadb_table_created_1.exists


@pytest.mark.postgresql
def test_postgresql_table_not_exists(
    postgresql_database_created_1: Generator[Database, None, None],
    postgresql_table_1: Generator[Table, None, None],
) -> None:
    assert not postgresql_table_1.exists


@pytest.mark.postgresql
def test_postgresql_table_not_exists_by_name(
    postgresql_database_created_1: Generator[Database, None, None],
    postgresql_table_created_1: Generator[Table, None, None],
    postgresql_table_2: Generator[Table, None, None],
) -> None:
    assert not Table(
        database=postgresql_database_created_1,
        name=generate_random_string(),
    ).exists


@pytest.mark.postgresql
def test_postgresql_table_exists(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    assert postgresql_table_created_1.exists


@pytest.mark.mariadb
def test_mariadb_table_name_with_schema_name(
    mariadb_table_1: Generator[Table, None, None],
) -> None:
    assert (
        mariadb_table_1._table_name_with_schema_name
        == f"{mariadb_table_1.database.name}.{mariadb_table_1.name}"
    )


@pytest.mark.postgresql
def test_postgresql_table_name_with_schema_name(
    postgresql_table_1: Generator[Table, None, None],
) -> None:
    assert (
        postgresql_table_1._table_name_with_schema_name
        == f"{postgresql_table_1.database.name}.{postgresql_table_1.name}"
    )


@pytest.mark.mariadb
def test_mariadb_table_not_drop_when_not_exists(
    mariadb_table_1: Generator[Table, None, None],
) -> None:
    assert not mariadb_table_1.drop()


@pytest.mark.mariadb
def test_mariadb_table_drop_when_exists(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    assert mariadb_table_created_1.drop()


@pytest.mark.postgresql
def test_postgresql_table_not_drop_when_not_exists(
    postgresql_table_1: Generator[Table, None, None],
) -> None:
    assert not postgresql_table_1.drop()


@pytest.mark.postgresql
def test_postgresql_table_drop_when_exists(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    assert postgresql_table_created_1.drop()


@pytest.mark.mariadb
def test_mariadb_table_checksum(
    mariadb_table_created_1: Generator[Database, None, None],
) -> None:
    assert mariadb_table_created_1.checksum == 0


@pytest.mark.postgresql
def test_postgresql_table_checksum_not_supported(
    postgresql_table_created_1: Generator[Database, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        postgresql_table_created_1.checksum


@pytest.mark.mariadb
def test_mariadb_table_reflection(
    mariadb_table_created_1: Generator[Database, None, None],
) -> None:
    assert isinstance(mariadb_table_created_1.reflection, SQLAlchemyTable)

    assert mariadb_table_created_1.reflection.name == mariadb_table_created_1.name
    assert (
        mariadb_table_created_1.reflection.fullname
        == f"{mariadb_table_created_1.database.name}.{mariadb_table_created_1.name}"
    )
    assert (
        mariadb_table_created_1.reflection.schema
        == mariadb_table_created_1.database.name
    )


@pytest.mark.postgresql
def test_postgresql_table_reflection(
    postgresql_table_created_1: Generator[Database, None, None],
) -> None:
    assert isinstance(postgresql_table_created_1.reflection, SQLAlchemyTable)

    assert postgresql_table_created_1.reflection.name == postgresql_table_created_1.name
    assert (
        postgresql_table_created_1.reflection.fullname
        == f"{postgresql_table_created_1.database.name}.{postgresql_table_created_1.name}"
    )
    assert (
        postgresql_table_created_1.reflection.schema
        == postgresql_table_created_1.database.name
    )
