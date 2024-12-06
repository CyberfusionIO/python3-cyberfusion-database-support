from typing import Generator

import pytest

from cyberfusion.DatabaseSupport.database_users import DatabaseUser
from cyberfusion.DatabaseSupport.exceptions import (
    InvalidInputError,
    PasswordMissingError,
)
from cyberfusion.DatabaseSupport.servers import Server
from cyberfusion.DatabaseSupport.utilities import generate_random_string


@pytest.mark.mariadb
def test_database_user_create_wrong_name(mariadb_server: Server) -> None:
    NAME = "mariad^b_testing"

    with pytest.raises(InvalidInputError) as e:
        DatabaseUser(
            server=mariadb_server,
            name=NAME,
            server_software_name="MariaDB",
            host="%",
        )

    assert e.value.input_ == NAME


@pytest.mark.mariadb
def test_database_user_name(mariadb_server: Server) -> None:
    NAME = generate_random_string()

    assert (
        DatabaseUser(
            server=mariadb_server,
            name=NAME,
            server_software_name="MariaDB",
            host="%",
        ).name
        == NAME
    )


@pytest.mark.mariadb
def test_database_user_host(mariadb_server: Server) -> None:
    HOST = "%"

    assert (
        DatabaseUser(
            server=mariadb_server,
            name=generate_random_string(),
            server_software_name="MariaDB",
            host=HOST,
        ).host
        == HOST
    )


@pytest.mark.postgresql
def test_postgresql_database_user_host(
    postgresql_server: Server,
) -> None:
    assert (
        DatabaseUser(
            server=postgresql_server,
            name=generate_random_string(),
            server_software_name="PostgreSQL",
            host="%",
        ).host
        is None  # Host overridden when using PostgreSQL
    )


@pytest.mark.mariadb
def test_mariadb_database_user_not_drop_when_not_exists(
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert not mariadb_database_user.drop()


@pytest.mark.mariadb
def test_mariadb_database_user_drop_when_exists(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert mariadb_database_user_created.drop()


@pytest.mark.mariadb
def test_mariadb_database_user_create_when_not_exists(
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert mariadb_database_user.create()


@pytest.mark.mariadb
def test_mariadb_database_user_not_create_when_exists(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not mariadb_database_user_created.create()


@pytest.mark.mariadb
def test_mariadb_database_user_exists(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert mariadb_database_user_created.exists


@pytest.mark.mariadb
def test_mariadb_database_user_not_exists(
    mariadb_server: Server,
    mariadb_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert not mariadb_database_user.exists


@pytest.mark.mariadb
def test_mariadb_database_user_not_exists_by_name(
    mariadb_server: Server,
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUser(
        server=mariadb_server,
        # name=mariadb_database_user_created.name,
        name=generate_random_string(),
        server_software_name=mariadb_database_user_created.server_software_name,
        password=mariadb_database_user_created.password,
        host=mariadb_database_user_created.host,
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_not_exists_by_host(
    mariadb_server: Server,
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUser(
        server=mariadb_server,
        name=mariadb_database_user_created.name,
        server_software_name=mariadb_database_user_created.server_software_name,
        password=mariadb_database_user_created.password,
        # host=mariadb_database_user_created.host,
        host=generate_random_string(),
    ).exists


@pytest.mark.mariadb
def test_mariadb_database_user_create_without_password(
    mariadb_server: Server,
) -> None:
    with pytest.raises(PasswordMissingError):
        DatabaseUser(
            server=mariadb_server,
            name=generate_random_string(),
            server_software_name="MariaDB",
            host="%",
        ).create()


@pytest.mark.mariadb
def test_mariadb_database_user_edit_without_password(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    with pytest.raises(PasswordMissingError):
        DatabaseUser(
            server=mariadb_database_user_created.server,
            name=mariadb_database_user_created.name,
            server_software_name=mariadb_database_user_created.server_software_name,
            host=mariadb_database_user_created.host,
        ).edit()


@pytest.mark.mariadb
def test_mariadb_database_user_not_edit_original_password(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not mariadb_database_user_created.edit()


@pytest.mark.mariadb
def test_mariadb_database_user_edit_password(
    mariadb_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    PASSWORD = "*1955D82D03BFF58A54B3E00DB0C45B01F6792A67"

    mariadb_database_user_created.password = PASSWORD

    assert mariadb_database_user_created.edit()

    assert mariadb_database_user_created._get_password() == PASSWORD


@pytest.mark.postgresql
def test_postgresql_database_user_not_drop_when_not_exists(
    postgresql_database_user: DatabaseUser,
) -> None:
    assert not postgresql_database_user.drop()


@pytest.mark.postgresql
def test_postgresql_database_user_drop_when_exists(
    postgresql_database_user_created: DatabaseUser,
) -> None:
    assert postgresql_database_user_created.drop()


@pytest.mark.postgresql
def test_postgresql_database_user_create_when_not_exists(
    postgresql_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert postgresql_database_user.create()


@pytest.mark.postgresql
def test_postgresql_database_user_not_create_when_exists(
    postgresql_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not postgresql_database_user_created.create()


@pytest.mark.postgresql
def test_postgresql_database_user_exists(
    postgresql_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert postgresql_database_user_created.exists


@pytest.mark.postgresql
def test_postgresql_database_user_not_exists(
    postgresql_server: Server,
    postgresql_database_user: Generator[DatabaseUser, None, None],
) -> None:
    assert not postgresql_database_user.exists


@pytest.mark.postgresql
def test_postgresql_database_user_not_exists_by_name(
    postgresql_server: Server,
    postgresql_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not DatabaseUser(
        server=postgresql_server,
        # name=postgresql_database_user_created.name,
        name=generate_random_string(),
        server_software_name=postgresql_database_user_created.server_software_name,
        password=postgresql_database_user_created.password,
        host=postgresql_database_user_created.host,
    ).exists


# @pytest.mark.postgresql
# def test_postgresql_database_user_not_exists_by_host(
#     postgresql_server: Server,
#     postgresql_database_user_created: Generator[DatabaseUser, None, None],
# ) -> None:
#     assert not DatabaseUser(
#         server=postgresql_server,
#         name=postgresql_database_user_created.name,
#         server_software_name=postgresql_database_user_created.server_software_name,
#         password=postgresql_database_user_created.password,
#         # host=postgresql_database_user_created.host,
#         host=''
#     )


@pytest.mark.postgresql
def test_postgresql_database_user_not_edit_original_password(
    postgresql_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    assert not postgresql_database_user_created.edit()


@pytest.mark.postgresql
def test_postgresql_database_user_edit_password(
    postgresql_database_user_created: Generator[DatabaseUser, None, None],
) -> None:
    PASSWORD = "md549bdc475e7a78ec43cae5ee9e0c1ccf9"

    postgresql_database_user_created.password = PASSWORD

    assert postgresql_database_user_created.edit()

    assert postgresql_database_user_created._get_password() == PASSWORD
