from typing import Generator

import pytest
from sqlalchemy import Table as SQLAlchemyTable
from sqlalchemy.schema import Index

from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import (
    IndexExistsError,
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


@pytest.mark.mariadb
def test_mariadb_table_create_index(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    mariadb_table_created_1.create_index(
        name=index_name, columns=["user_name", "email_address"]
    )

    index_names = [index.name for index in mariadb_table_created_1.reflection.indexes]

    assert index_name in index_names


@pytest.mark.mariadb
def test_mariadb_table_create_index_with_other_index_existing(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    mariadb_table_created_1.create_index(
        name=generate_random_string(), columns=["user_name"]
    )

    second_index_name = generate_random_string()

    mariadb_table_created_1.create_index(
        name=second_index_name, columns=["email_address"]
    )

    index_names = [index.name for index in mariadb_table_created_1.reflection.indexes]

    assert second_index_name in index_names


@pytest.mark.mariadb
def test_mariadb_table_create_index_already_exists(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    mariadb_table_created_1.create_index(name=index_name, columns=["user_name"])

    with pytest.raises(IndexExistsError):
        mariadb_table_created_1.create_index(name=index_name, columns=["user_name"])


@pytest.mark.postgresql
def test_postgresql_table_create_index(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    postgresql_table_created_1.create_index(
        name=index_name, columns=["user_name", "email_address"]
    )

    index_names = [
        index.name for index in postgresql_table_created_1.reflection.indexes
    ]

    assert index_name in index_names


@pytest.mark.postgresql
def test_postgresql_table_create_index_with_other_index_existing(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    postgresql_table_created_1.create_index(
        name=generate_random_string(), columns=["user_name"]
    )

    second_index_name = generate_random_string()

    postgresql_table_created_1.create_index(
        name=second_index_name, columns=["email_address"]
    )

    index_names = [
        index.name for index in postgresql_table_created_1.reflection.indexes
    ]

    assert second_index_name in index_names


@pytest.mark.postgresql
def test_postgresql_table_create_index_already_exists(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    postgresql_table_created_1.create_index(name=index_name, columns=["user_name"])

    with pytest.raises(IndexExistsError):
        postgresql_table_created_1.create_index(name=index_name, columns=["user_name"])


@pytest.mark.mariadb
def test_mariadb_table_get_indexes_by_column(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    mariadb_table_created_1.create_index(
        name=index_name, columns=["user_name", "email_address"]
    )

    indexes = mariadb_table_created_1.get_indexes_by_column(column="user_name")

    assert len(indexes) == 1

    assert isinstance(indexes[0], Index)

    assert indexes[0].name == index_name


@pytest.mark.mariadb
def test_mariadb_table_get_indexes_by_column_no_match(
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    mariadb_table_created_1.create_index(
        name=generate_random_string(), columns=["user_name"]
    )

    assert mariadb_table_created_1.get_indexes_by_column(column="email_address") == []


@pytest.mark.postgresql
def test_postgresql_table_get_indexes_by_column(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    index_name = generate_random_string()

    postgresql_table_created_1.create_index(
        name=index_name, columns=["user_name", "email_address"]
    )

    indexes = postgresql_table_created_1.get_indexes_by_column(column="user_name")

    assert len(indexes) == 1

    assert isinstance(indexes[0], Index)

    assert indexes[0].name == index_name


@pytest.mark.postgresql
def test_postgresql_table_get_indexes_by_column_no_match(
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    postgresql_table_created_1.create_index(
        name=generate_random_string(), columns=["user_name"]
    )

    assert (
        postgresql_table_created_1.get_indexes_by_column(column="email_address") == []
    )
