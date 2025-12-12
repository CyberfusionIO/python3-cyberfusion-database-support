import os
import shutil
import uuid
from typing import Generator

import pytest
from _pytest.config.argparsing import Parser
from pytest_mock import MockerFixture  # type: ignore[attr-defined]
from sqlalchemy import Column, Integer, String
from sqlalchemy import Table as SQLAlchemyTable
from sqlalchemy.schema import CreateSchema, DropSchema

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.database_importation import (
    DatabaseImportation,
)
from cyberfusion.DatabaseSupport.database_user_grants import DatabaseUserGrant
from cyberfusion.DatabaseSupport.database_users import DatabaseUser
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.servers import Server
from cyberfusion.DatabaseSupport.tables import Table
from cyberfusion.DatabaseSupport.utilities import generate_random_string
from tests._utilities import add_worker_id_to_server_host


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--mariadb-server-host", action="store", required=True)
    parser.addoption("--postgresql-server-host", action="store", required=True)
    parser.addoption("--mariadb-server-password", action="store", required=True)
    parser.addoption("--postgresql-server-password", action="store", required=True)


@pytest.fixture
def mariadb_server_host(request: pytest.FixtureRequest, worker_id: str) -> str:
    return add_worker_id_to_server_host(
        request.config.getoption("--mariadb-server-host"), worker_id
    )


@pytest.fixture
def postgresql_server_host(request: pytest.FixtureRequest, worker_id: str) -> str:
    return add_worker_id_to_server_host(
        request.config.getoption("--postgresql-server-host"), worker_id
    )


@pytest.fixture
def mariadb_server_password(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--mariadb-server-password")


@pytest.fixture
def postgresql_server_password(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--postgresql-server-password")


@pytest.fixture(autouse=True)
def dump_directory() -> Generator[str, None, None]:
    path = os.path.join(os.path.sep, "tmp", str(uuid.uuid4()))

    os.mkdir(path)

    yield path

    shutil.rmtree(path)


@pytest.fixture
def local_support(mariadb_support: DatabaseSupport) -> DatabaseSupport:
    return mariadb_support


@pytest.fixture
def mariadb_support(
    mariadb_server_password: str, mariadb_server_host: str
) -> DatabaseSupport:
    return DatabaseSupport(
        server_software_names=["MariaDB"],
        server_password=mariadb_server_password,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username="root",
    )


@pytest.fixture
def postgresql_support(
    postgresql_server_password: str, postgresql_server_host: str
) -> DatabaseSupport:
    return DatabaseSupport(
        server_software_names=["PostgreSQL"],
        server_password=postgresql_server_password,
        postgresql_server_host=postgresql_server_host,
        postgresql_server_username="postgres",
    )


@pytest.fixture
def mariadb_server(mariadb_support: DatabaseSupport) -> Server:
    return Server(support=mariadb_support)


@pytest.fixture
def postgresql_server(postgresql_support: DatabaseSupport) -> Server:
    return Server(support=postgresql_support)


@pytest.fixture
def database_importation(
    mocker: MockerFixture,
    mariadb_support: DatabaseSupport,
    mariadb_database_created_1: Generator[Database, None, None],
) -> Generator[DatabaseImportation, None, None]:
    database_importation = DatabaseImportation(
        privileged_support=mariadb_support,
        database_name=mariadb_database_created_1.name,
        server_software_name=mariadb_database_created_1.server_software_name,
        source_path="tests/dumps/deviating_tables_1.sql",
    )

    # Hardcoded to localhost, but server may be on a different hostname in CI

    mocker.patch(
        "cyberfusion.DatabaseSupport.database_importation.DatabaseImportation.NAME_ACCESS_HOST",
        "%",
    )

    yield database_importation

    try:
        database_importation._delete_objects()
    except Exception:
        # Objects may not exist if they were not created during test

        pass


# MariaDB database


@pytest.fixture
def mariadb_database(
    mariadb_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=mariadb_server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    yield database

    if database.exists:
        database.drop()


@pytest.fixture
def mariadb_database_with_wrong_name(
    mariadb_server: Server, worker_id: str
) -> Generator[Database, None, None]:
    database = Database(
        support=mariadb_server.support,
        name=f"mariadb*test_{worker_id}",
        server_software_name="MariaDB",
    )

    yield database

    if database.exists:
        database.drop()


@pytest.fixture
def mariadb_database_created_1(
    mariadb_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=mariadb_server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    database.create()

    yield database

    if database.exists:
        database.drop()


@pytest.fixture
def mariadb_database_created_2(
    mariadb_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=mariadb_server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    database.create()

    yield database

    if database.exists:
        database.drop()


# PostgreSQL database


@pytest.fixture
def postgresql_database(
    postgresql_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=postgresql_server.support,
        name=generate_random_string(),
        server_software_name="PostgreSQL",
    )

    yield database

    if database.exists:
        database.drop()


@pytest.fixture
def postgresql_database_created_1(
    postgresql_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=postgresql_server.support,
        name=generate_random_string(),
        server_software_name="PostgreSQL",
    )

    database.create()

    yield database

    if database.exists:
        database.drop()


@pytest.fixture
def postgresql_database_created_2(
    postgresql_server: Server,
) -> Generator[Database, None, None]:
    database = Database(
        support=postgresql_server.support,
        name=generate_random_string(),
        server_software_name="PostgreSQL",
    )

    database.create()

    yield database

    if database.exists:
        database.drop()


# MariaDB table


@pytest.fixture
def mariadb_table_1(
    mariadb_database_created_1: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    table = Table(
        database=mariadb_database_created_1,
        name=generate_random_string(),
    )

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def mariadb_table_2(
    mariadb_database_created_1: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    table = Table(database=mariadb_database_created_1, name=generate_random_string())

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def mariadb_table_created_1(
    mariadb_database_created_1: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    name = generate_random_string()

    table = Table(
        database=mariadb_database_created_1,
        name=name,
    )

    engine = mariadb_database_created_1.database_engine

    SQLAlchemyTable(
        name,
        mariadb_database_created_1.metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
        schema=mariadb_database_created_1.name,
    ).create(engine)

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def mariadb_table_created_2(
    mariadb_database_created_1: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    name = generate_random_string()

    table = Table(
        database=mariadb_database_created_1,
        name=name,
    )

    engine = mariadb_database_created_1.database_engine

    SQLAlchemyTable(
        name,
        mariadb_database_created_1.metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
        schema=mariadb_database_created_1.name,
    ).create(engine)

    yield table

    if table.exists:
        table.drop()


# PostgreSQL schema


@pytest.fixture
def postgresql_schema_created(
    postgresql_database_created_1: Generator[Database, None, None],
) -> Generator[str, None, None]:
    name = postgresql_database_created_1.name

    with postgresql_database_created_1.database_engine.begin() as connection:
        connection.execute(CreateSchema(name))

    yield name

    with postgresql_database_created_1.database_engine.begin() as connection:
        connection.execute(DropSchema(name))


# PostgreSQL table


@pytest.fixture
def postgresql_table_1(
    postgresql_database_created_1: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    table = Table(database=postgresql_database_created_1, name=generate_random_string())

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def postgresql_table_created_1(
    postgresql_database_created_1: Generator[Database, None, None],
    postgresql_schema_created: Generator[str, None, None],
) -> Generator[Table, None, None]:
    name = generate_random_string()

    table = Table(
        database=postgresql_database_created_1,
        name=name,
    )

    engine = postgresql_database_created_1.database_engine

    SQLAlchemyTable(
        name,
        postgresql_database_created_1.metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
        schema=postgresql_schema_created,
    ).create(engine)

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def postgresql_table_2(
    postgresql_database_created_2: Generator[Database, None, None],
) -> Generator[Table, None, None]:
    table = Table(database=postgresql_database_created_2, name=generate_random_string())

    yield table

    if table.exists:
        table.drop()


@pytest.fixture
def postgresql_table_created_2(
    postgresql_database_created_2: Generator[Database, None, None],
    postgresql_schema_created: Generator[str, None, None],
) -> Generator[Table, None, None]:
    name = generate_random_string()

    table = Table(
        database=postgresql_database_created_2,
        name=name,
    )

    engine = postgresql_database_created_2.database_engine

    SQLAlchemyTable(
        name,
        postgresql_database_created_2.metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
        schema=postgresql_schema_created,
    ).create(engine)

    yield table

    if table.exists:
        table.drop()


# MariaDB database user


@pytest.fixture
def mariadb_database_user(
    mariadb_server: Server,
) -> Generator[DatabaseUser, None, None]:
    database_user = DatabaseUser(
        server=mariadb_server,
        name=generate_random_string(),
        server_software_name="MariaDB",
        password="*AC57754462B6D4C373263062D60EDC6E452E574D",
        host="%",
    )

    yield database_user

    if database_user.exists:
        database_user.drop()


@pytest.fixture
def mariadb_database_user_created(
    mariadb_server: Server,
) -> Generator[DatabaseUser, None, None]:
    database_user = DatabaseUser(
        server=mariadb_server,
        name=generate_random_string(),
        server_software_name="MariaDB",
        password="*AC57754462B6D4C373263062D60EDC6E452E574D",
        host="%",
    )

    database_user.create()

    yield database_user

    if database_user.exists:
        database_user.drop()


# PostgreSQL database user


@pytest.fixture
def postgresql_database_user(
    postgresql_server: Server,
) -> Generator[DatabaseUser, None, None]:
    database_user = DatabaseUser(
        server=postgresql_server,
        name=generate_random_string(),
        server_software_name="PostgreSQL",
        password="md5436765032c9c4df46ab3ac512f98cb9e",
        host=None,
    )

    yield database_user

    if database_user.exists:
        database_user.drop()


@pytest.fixture
def postgresql_database_user_created(
    postgresql_server: Server,
) -> Generator[DatabaseUser, None, None]:
    database_user = DatabaseUser(
        server=postgresql_server,
        name=generate_random_string(),
        server_software_name="PostgreSQL",
        password="md5436765032c9c4df46ab3ac512f98cb9e",
        host=None,
    )

    database_user.create()

    yield database_user

    if database_user.exists:
        database_user.drop()


# MariaDB database user grant


@pytest.fixture
def mariadb_database_user_grant(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> Generator[DatabaseUserGrant, None, None]:
    database_user_grant = DatabaseUserGrant(
        database=mariadb_database_created_1,
        database_user=mariadb_database_user_created,
        privilege_names=["ALL"],
        table=None,
    )

    yield database_user_grant

    if database_user_grant.exists:
        database_user_grant.revoke()


@pytest.fixture
def mariadb_database_user_grant_created(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> Generator[DatabaseUserGrant, None, None]:
    database_user_grant = DatabaseUserGrant(
        database=mariadb_database_created_1,
        database_user=mariadb_database_user_created,
        privilege_names=["ALL"],
        table=None,
    )

    database_user_grant.grant()

    yield database_user_grant

    if database_user_grant.exists:
        database_user_grant.revoke()
