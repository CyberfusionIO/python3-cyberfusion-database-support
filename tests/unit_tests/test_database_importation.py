from typing import Generator

import pytest

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.database_importation import (
    DatabaseImportation,
)
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.servers import Server


@pytest.mark.mariadb
def test_mariadb_database_importation_attributes(
    mariadb_support: DatabaseSupport,
    mariadb_database_created_1: Generator[Database, None, None],
    database_importation: Generator[DatabaseImportation, None, None],
) -> None:
    # Basic attributes

    assert (
        database_importation.database_name == mariadb_database_created_1.name
    )
    assert database_importation.source_path == "mariadb_testing_1.sql"
    assert (
        database_importation.server_software_name
        == mariadb_database_created_1.server_software_name
    )
    assert database_importation.username.startswith("restore-")
    database_importation.unhashed_password
    database_importation.hashed_password

    # Unprivileged support

    assert database_importation.unprivileged_support.server_software_names == [
        mariadb_database_created_1.server_software_name,
    ]
    assert (
        database_importation.unprivileged_support.server_password
        == database_importation.unhashed_password
    )
    assert (
        database_importation.unprivileged_support.mariadb_server_username
        == database_importation.username
    )
    assert (
        database_importation.unprivileged_support.mariadb_server_host
        == database_importation.privileged_support.mariadb_server_host
    )

    assert (
        database_importation.unprivileged_database.name
        == mariadb_database_created_1.name
    )
    assert (
        database_importation.unprivileged_database.server_software_name
        == mariadb_database_created_1.server_software_name
    )
    assert (
        database_importation.unprivileged_database.support
        == database_importation.unprivileged_support
    )

    # Privileged

    assert database_importation.privileged_support == mariadb_support

    assert (
        database_importation.privileged_server.support
        == database_importation.privileged_support
    )

    assert (
        database_importation.privileged_database.name
        == mariadb_database_created_1.name
    )
    assert (
        database_importation.privileged_database.server_software_name
        == mariadb_database_created_1.server_software_name
    )
    assert (
        database_importation.privileged_database.support
        == database_importation.privileged_support
    )

    assert (
        database_importation.database_user.server
        == database_importation.privileged_server
    )
    assert (
        database_importation.database_user.name
        == database_importation.username
    )
    assert (
        database_importation.database_user.server_software_name
        == mariadb_database_created_1.server_software_name
    )
    assert (
        database_importation.database_user.password
        == database_importation.hashed_password
    )
    assert database_importation.database_user.host == "%"

    assert (
        database_importation.database_user_grant.database
        == database_importation.privileged_database
    )
    assert (
        database_importation.database_user_grant.database_user
        == database_importation.database_user
    )
    assert database_importation.database_user_grant.privilege_names == ["ALL"]
    assert database_importation.database_user_grant.table_name == "*"


@pytest.mark.mariadb
def test_mariadb_database_importation_objects(
    mariadb_support: DatabaseSupport,
    mariadb_server: Server,
    mariadb_database_created_1: Generator[Database, None, None],
    database_importation,
) -> None:
    # Create

    database_importation._create_objects()

    assert any(
        database_importation.username == database_user.name
        and database_importation.server_software_name
        == database_user.server_software_name
        and database_importation.hashed_password == database_user.password
        and "%" == database_user.host
        for database_user in mariadb_server.database_users
    )

    assert any(
        database_importation.privileged_database.support
        == database_user_grant.database.support
        and database_importation.privileged_database.name
        == database_user_grant.database.name
        and database_importation.privileged_database.server_software_name
        == database_user_grant.database.server_software_name
        and database_importation.database_user.name
        == database_user_grant.database_user.name
        and database_importation.database_user.server_software_name
        == database_user_grant.database_user.server_software_name
        and database_importation.database_user.password
        == database_user_grant.database_user.password
        and "%" == database_user_grant.database_user.host
        and ["ALL"] == database_user_grant.privilege_names
        and "*" == database_user_grant.table_name
        for database_user_grant in mariadb_server.database_user_grants
    )

    # Delete

    database_importation._delete_objects()

    assert not any(
        database_importation.username == database_user.name
        and database_importation.server_software_name
        == database_user.server_software_name
        and database_importation.hashed_password == database_user.password
        and "%" == database_user.host
        for database_user in mariadb_server.database_users
    )

    assert not any(
        database_importation.privileged_database.support
        == database_user_grant.database.support
        and database_importation.privileged_database.name
        == database_user_grant.database.name
        and database_importation.privileged_database.server_software_name
        == database_user_grant.database.server_software_name
        and database_importation.database_user.name
        == database_user_grant.database_user.name
        and database_importation.database_user.server_software_name
        == database_user_grant.database_user.server_software_name
        and database_importation.database_user.password
        == database_user_grant.database_user.password
        and "%" == database_user_grant.database_user.host
        and ["ALL"] == database_user_grant.privilege_names
        and "*" == database_user_grant.table_name
        for database_user_grant in mariadb_server.database_user_grants
    )
