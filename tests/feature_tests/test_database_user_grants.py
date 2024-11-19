from typing import Generator

import pytest

from cyberfusion.DatabaseSupport.database_user_grants import DatabaseUserGrant
from cyberfusion.DatabaseSupport.database_users import DatabaseUser
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import (
    InvalidInputError,
    ServerNotSupportedError,
)
from cyberfusion.DatabaseSupport.servers import Server
from cyberfusion.DatabaseSupport.tables import Table
from cyberfusion.DatabaseSupport.utilities import generate_random_string


@pytest.mark.mariadb
def test_mariadb_database_user_grant_database_name(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=None,
        ).database_name
        == mariadb_database.name
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_privilege_names(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert DatabaseUserGrant(
        database=mariadb_database,
        database_user=mariadb_database_user,
        privilege_names=["ALL", "SELECT"],
        table=None,
    ).privilege_names == ["ALL", "SELECT"]


@pytest.mark.mariadb
def test_database_user_grant_create_wrong_database_name(
    mariadb_database_with_wrong_name: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    with pytest.raises(InvalidInputError) as e:
        DatabaseUserGrant(
            database=mariadb_database_with_wrong_name,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=None,
        )

    assert e.value.input_ == mariadb_database_with_wrong_name.name


@pytest.mark.mariadb
def test_database_user_grant_create_wrong_privilege_name(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    PRIVILEGE_NAME = "AL*L"

    with pytest.raises(InvalidInputError) as e:
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=[PRIVILEGE_NAME],
            table=None,
        )

    assert e.value.input_ == PRIVILEGE_NAME


@pytest.mark.postgresql
def test_postgresql_database_user_grant_not_supported(
    postgresql_database: Generator[Database, None, None],
    postgresql_database_user: Generator[DatabaseUser, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        DatabaseUserGrant(
            database=postgresql_database,
            database_user=postgresql_database_user,
            privilege_names=["ALL"],
            table=None,
        )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_text_privilege_names(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    PRIVILEGE_NAME_ALL = "ALL"
    PRIVILEGE_NAME_SELECT = "SELECT"

    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=[PRIVILEGE_NAME_ALL, PRIVILEGE_NAME_SELECT],
            table=None,
        ).text_privilege_names
        == f"{PRIVILEGE_NAME_ALL}, {PRIVILEGE_NAME_SELECT}"
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_no_table_name_wildcard(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=None,
        ).table_name
        == "*"
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_table_name_not_wildcard(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
    mariadb_table_1: Generator[Table, None, None],
) -> None:
    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=mariadb_table_1,
        ).table_name
        == mariadb_table_1.name
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_text_table_name_wildcard(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=None,
        ).text_table_name
        == "*"
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_text_table_name_not_wilcard(
    mariadb_database: Generator[Database, None, None],
    mariadb_database_user: Generator[DatabaseUser, None, None],
    mariadb_table_1: Generator[Table, None, None],
) -> None:
    assert (
        DatabaseUserGrant(
            database=mariadb_database,
            database_user=mariadb_database_user,
            privilege_names=["ALL"],
            table=mariadb_table_1,
        ).text_table_name
        == f"`{mariadb_table_1.name}`"
    )


@pytest.mark.mariadb
def test_mariadb_database_user_grant_create_when_not_exists(
    mariadb_database_user_grant: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert mariadb_database_user_grant.create()


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_create_when_exists(
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert not mariadb_database_user_grant_created.create()


@pytest.mark.mariadb
def test_mariadb_database_user_grant_exists(
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert mariadb_database_user_grant_created.exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists(
    mariadb_server: Server,
    mariadb_database_user_grant: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert not mariadb_database_user_grant.exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists_by_table_name(
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    assert not DatabaseUserGrant(
        database=mariadb_database_user_grant_created.database,
        database_user=mariadb_database_user_grant_created.database_user,
        privilege_names=mariadb_database_user_grant_created.privilege_names,
        # table=None,
        table=mariadb_table_created_1,  # Different table will change value of table_name
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists_by_privilege_names(
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert not DatabaseUserGrant(
        database=mariadb_database_user_grant_created.database,
        database_user=mariadb_database_user_grant_created.database_user,
        # privilege_names=mariadb_database_user_grant_created.privilege_names,
        privilege_names=[generate_random_string()],
        table=None,
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists_by_database_user_name(
    mariadb_server: Server,
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUserGrant(
        database=mariadb_database_user_grant_created.database,
        # database_user=mariadb_database_user_grant_created.database_user,
        database_user=DatabaseUser(
            server=mariadb_server,
            name=generate_random_string(),
            server_software_name=mariadb_database_user_created.server_software_name,
            host=mariadb_database_user_created.host,
        ),
        privilege_names=mariadb_database_user_grant_created.privilege_names,
        table=None,
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists_by_database_user_host(
    mariadb_server: Server,
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUserGrant(
        database=mariadb_database_user_grant_created.database,
        # database_user=mariadb_database_user_grant_created.database_user,
        database_user=DatabaseUser(
            server=mariadb_server,
            name=mariadb_database_user_grant_created.database_user.name,
            server_software_name=mariadb_database_user_created.server_software_name,
            host=generate_random_string(),
        ),
        privilege_names=mariadb_database_user_grant_created.privilege_names,
        table=None,
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_exists_by_database(
    mariadb_server: Server,
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUserGrant(
        database=Database(
            support=mariadb_server.support,
            name=generate_random_string(),
            server_software_name=mariadb_database_user_created.server_software_name,
        ),
        # database=mariadb_database_user_grant_created.database,
        database_user=mariadb_database_user_grant_created.database_user,
        privilege_names=mariadb_database_user_grant_created.privilege_names,
        table=None,
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_grant_not_delete_when_not_exists(
    mariadb_database_user_grant: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert not mariadb_database_user_grant.delete()


@pytest.mark.mariadb
def test_mariadb_database_user_grant_delete_when_exists(
    mariadb_database_user_grant_created: Generator[DatabaseUserGrant, None, None],
) -> None:
    assert mariadb_database_user_grant_created.delete()
