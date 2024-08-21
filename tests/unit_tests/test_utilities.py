from cyberfusion.DatabaseSupport.utilities import (
    _generate_mariadb_dsn,
    get_host_is_socket,
)
from tests._utilities import generate_random_string


def test_generate_mariadb_dsn_tcp_without_database_name() -> None:
    USERNAME = "root"
    HOST = "localhost"
    PASSWORD = generate_random_string()

    assert (
        _generate_mariadb_dsn(
            username=USERNAME,
            host=HOST,
            password=PASSWORD,
            database_name=None,
        )
        == f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}"
    )


def test_generate_mariadb_dsn_socket_without_database_name() -> None:
    USERNAME = "root"
    HOST = "/run/mysqld/mysqld.sock"
    PASSWORD = generate_random_string()

    assert (
        _generate_mariadb_dsn(
            username=USERNAME,
            host=HOST,
            password=PASSWORD,
            database_name=None,
        )
        == f"mysql+pymysql://{USERNAME}:{PASSWORD}@/?unix_socket={HOST}"
    )


def test_generate_mariadb_dsn_tcp_with_database_name() -> None:
    USERNAME = "root"
    HOST = "localhost"
    PASSWORD = generate_random_string()
    DATABASE_NAME = generate_random_string()

    assert (
        _generate_mariadb_dsn(
            username=USERNAME,
            host=HOST,
            password=PASSWORD,
            database_name=DATABASE_NAME,
        )
        == f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE_NAME}"
    )


def test_generate_mariadb_dsn_socket_with_database_name() -> None:
    USERNAME = "root"
    HOST = "/run/mysqld/mysqld.sock"
    PASSWORD = generate_random_string()
    DATABASE_NAME = generate_random_string()

    assert (
        _generate_mariadb_dsn(
            username=USERNAME,
            host=HOST,
            password=PASSWORD,
            database_name=DATABASE_NAME,
        )
        == f"mysql+pymysql://{USERNAME}:{PASSWORD}@/{DATABASE_NAME}?unix_socket={HOST}"
    )


def test_get_host_is_socket_true() -> None:
    assert get_host_is_socket("/run/mysqld/mysqld.sock")


def test_get_host_is_socket_false() -> None:
    assert not get_host_is_socket("localhost")
