from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa
from pytest_mock import MockerFixture  # type: ignore[attr-defined]

from cyberfusion.DatabaseSupport.database_users import DatabaseUser
from cyberfusion.DatabaseSupport.servers import Server
from cyberfusion.DatabaseSupport.utilities import generate_random_string

# Databases


@pytest.mark.mariadb
def test_mariadb_databases(mocker: MockerFixture, mariadb_server: Server) -> None:
    NAME = generate_random_string()

    mocker.patch(
        "sqlalchemy.engine.reflection.Inspector.get_schema_names",
        return_value=[
            "mysql",
            "sys",
            "performance_schema",
            "information_schema",
            NAME,
        ],
    )

    assert len(mariadb_server.databases) == 1

    assert mariadb_server.databases[0].name == NAME
    assert mariadb_server.databases[0].server_software_name == "MariaDB"

    assert not any(
        database.name == "information_schema" for database in mariadb_server.databases
    )
    assert not any(
        database.name == "performance_schema" for database in mariadb_server.databases
    )
    assert not any(database.name == "mysql" for database in mariadb_server.databases)
    assert not any(database.name == "sys" for database in mariadb_server.databases)


@pytest.mark.postgresql
def test_postgresql_databases(mocker: MockerFixture, postgresql_server: Server) -> None:
    NAME = generate_random_string()

    mocker.patch(
        "cyberfusion.DatabaseSupport.queries.Query.result",
        new=mocker.PropertyMock(
            return_value=[
                ("template0",),
                ("template1",),
                ("postgres",),
                (NAME,),
            ]
        ),
    )

    assert len(postgresql_server.databases) == 1

    assert postgresql_server.databases[0].name == NAME
    assert postgresql_server.databases[0].server_software_name == "PostgreSQL"

    assert not any(
        database.name == "template0" for database in postgresql_server.databases
    )
    assert not any(
        database.name == "template1" for database in postgresql_server.databases
    )
    assert not any(
        database.name == "postgres" for database in postgresql_server.databases
    )


# Database users


@pytest.mark.mariadb
def test_mariadb_database_users(mocker: MockerFixture, mariadb_server: Server) -> None:
    NAME_1 = generate_random_string()
    HOST_1 = "%"
    PASSWORD_1 = "*E6CC90B878B948C35E92B003C792C46C58C4AF40"

    NAME_2 = generate_random_string()
    HOST_2 = "localhost"
    PASSWORD_2 = "*12033B78389744F3F39AC4CE4CCFCAD6960D8EA0"

    mocker.patch(
        "cyberfusion.DatabaseSupport.queries.Query.result",
        new=mocker.PropertyMock(
            return_value=[
                (
                    NAME_1,
                    HOST_1,
                    PASSWORD_1,
                ),
                (
                    NAME_2,
                    HOST_2,
                    PASSWORD_2,
                ),
                ("root", "localhost", ""),
                ("mysql", "localhost", "invalid"),
                ("mariadb.sys", "localhost", ""),
                ("debian-sys-maint", "localhost", ""),
                ("monitoring", "localhost", ""),
            ]
        ),
    )

    assert len(mariadb_server.database_users) == 3

    assert mariadb_server.database_users[0].name == NAME_1
    assert mariadb_server.database_users[0].server_software_name == "MariaDB"
    assert mariadb_server.database_users[0].password == PASSWORD_1
    assert mariadb_server.database_users[0].host == HOST_1

    assert mariadb_server.database_users[1].name == NAME_2
    assert mariadb_server.database_users[1].server_software_name == "MariaDB"
    assert mariadb_server.database_users[1].password == PASSWORD_2
    assert mariadb_server.database_users[2].host == HOST_2

    assert mariadb_server.database_users[2].name == "root"
    assert mariadb_server.database_users[2].server_software_name == "MariaDB"
    assert mariadb_server.database_users[2].password == ""
    assert mariadb_server.database_users[2].host == "localhost"

    assert not any(
        database.name == "monitoring" for database in mariadb_server.database_users
    )
    assert not any(
        database.name == "debian-sys-maint"
        for database in mariadb_server.database_users
    )
    assert not any(
        database.name == "mariadb.sys" for database in mariadb_server.database_users
    )
    assert not any(
        database.name == "mysql" for database in mariadb_server.database_users
    )


@pytest.mark.postgresql
def test_postgresql_database_users(
    mocker: MockerFixture, postgresql_server: Server
) -> None:
    NAME_1 = generate_random_string()
    NAME_2 = generate_random_string()

    PASSWORD = "md549bdc475e7a78ec43cae5ee9e0c1ccf9"

    class Query:
        """Abstract representation of database query."""

        def __init__(self, *, engine: sa.engine.base.Engine, query: str) -> None:
            """Set attributes and call functions to handle query."""
            self.engine = engine
            self.query = query

        @property
        def result(self) -> Any:
            """Set result."""
            if str(self.query).startswith("SELECT rolname"):
                return [
                    (NAME_1,),
                    (NAME_2,),
                    ("root",),
                    ("admin",),
                    ("postgres",),
                    ("pg_test",),
                ]

            elif str(self.query).startswith("SELECT rolpassword"):
                return [(PASSWORD,)]

            return mocker.DEFAULT

    mocker.patch(
        "cyberfusion.DatabaseSupport.servers.Query", new=Query
    )  # Can't patch individual property, see: https://github.com/python/cpython/issues/100291

    assert len(postgresql_server.database_users) == 3

    assert postgresql_server.database_users[0].name == NAME_1
    assert postgresql_server.database_users[0].server_software_name == "PostgreSQL"
    assert postgresql_server.database_users[0].password == PASSWORD
    assert postgresql_server.database_users[0].host is None

    assert postgresql_server.database_users[1].name == NAME_2
    assert postgresql_server.database_users[1].server_software_name == "PostgreSQL"
    assert postgresql_server.database_users[1].password == PASSWORD
    assert postgresql_server.database_users[1].host is None

    assert postgresql_server.database_users[2].name == "root"
    assert postgresql_server.database_users[2].server_software_name == "PostgreSQL"
    assert postgresql_server.database_users[2].password == PASSWORD
    assert postgresql_server.database_users[2].host is None

    assert not any(
        database.name == "admin" for database in postgresql_server.database_users
    )
    assert not any(
        database.name == "postgres" for database in postgresql_server.database_users
    )
    assert not any(
        database.name == "pg_" for database in postgresql_server.database_users
    )


# Database user grants


@pytest.mark.mariadb
def test_mariadb_database_user_grants(
    mocker: MockerFixture, mariadb_server: Server
) -> None:
    DATABASE_USER_NAME = "testuser1"
    DATABASE_NAME = "testdb"
    TABLE_NAME = "specifictable"

    mocker.patch(
        "cyberfusion.DatabaseSupport.queries.Query._execute",
        new=mocker.Mock(),
    )

    mocker.patch(
        "cyberfusion.DatabaseSupport.servers.Server.database_users",
        new=mocker.PropertyMock(
            return_value=[
                DatabaseUser(
                    server=mariadb_server,
                    name=DATABASE_USER_NAME,
                    server_software_name="MariaDB",
                    password=generate_random_string(),
                    host="%",
                )
            ]
        ),
    )

    mocker.patch(
        "cyberfusion.DatabaseSupport.queries.Query.result",
        new=mocker.PropertyMock(
            return_value=[
                (
                    f"GRANT USAGE ON *.* TO `{DATABASE_USER_NAME}`@`localhost` IDENTIFIED BY PASSWORD '*9F69E47E519D9CA02116BF5796684F7D0D45F8FA'",
                ),
                (
                    f"GRANT SELECT, EXECUTE, SHOW VIEW ON `{DATABASE_NAME}`.* TO `{DATABASE_USER_NAME}`@`localhost`",
                ),
                (
                    f"GRANT ALL PRIVILEGES ON `{DATABASE_NAME}`.`{TABLE_NAME}` TO `{DATABASE_USER_NAME}`@`localhost`",
                ),
            ]
        ),
    )

    assert len(mariadb_server.database_user_grants) == 3

    assert mariadb_server.database_user_grants[0].database.name == "*"
    assert (
        mariadb_server.database_user_grants[0].database_user.name == DATABASE_USER_NAME
    )
    assert mariadb_server.database_user_grants[0].privilege_names == ["USAGE"]
    assert mariadb_server.database_user_grants[0].table_name == "*"

    assert mariadb_server.database_user_grants[1].database.name == DATABASE_NAME
    assert (
        mariadb_server.database_user_grants[1].database_user.name == DATABASE_USER_NAME
    )
    assert mariadb_server.database_user_grants[1].privilege_names == [
        "SELECT",
        "EXECUTE",
        "SHOW VIEW",
    ]
    assert mariadb_server.database_user_grants[1].table_name == "*"

    assert mariadb_server.database_user_grants[2].database.name == DATABASE_NAME
    assert (
        mariadb_server.database_user_grants[2].database_user.name == DATABASE_USER_NAME
    )
    assert mariadb_server.database_user_grants[2].privilege_names == ["ALL"]
    assert mariadb_server.database_user_grants[2].table_name == TABLE_NAME


@pytest.mark.postgresql
def test_postgresql_database_user_grants(
    postgresql_server: Server,
) -> None:
    assert not postgresql_server.database_user_grants  # Not supported
