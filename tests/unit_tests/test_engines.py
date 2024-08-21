import os

from cyberfusion.DatabaseSupport import DatabaseSupport


def test_mariadb_engine_url_with_server_password(
    mariadb_server_password: str, mariadb_server_host: str
) -> None:
    mariadb_support = DatabaseSupport(
        server_software_names=["MariaDB"],
        server_password=mariadb_server_password,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username="root",
    )

    assert (
        mariadb_support.engines._mariadb_url
        == f"mysql+pymysql://root:{mariadb_server_password}@{mariadb_server_host}"
    )


def test_mariadb_engine_url_without_server_password(
    mariadb_server_host: str,
) -> None:
    mariadb_support = DatabaseSupport(
        server_software_names=["MariaDB"],
        server_password=None,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username="root",
    )

    assert (
        mariadb_support.engines._mariadb_url
        == f"mysql+pymysql://root@{mariadb_server_host}"
    )


def test_postgresql_engine_url_with_server_password(
    postgresql_server_password: str, postgresql_server_host: str
) -> None:
    postgresql_support = DatabaseSupport(
        server_software_names=["PostgreSQL"],
        server_password=postgresql_server_password,
        postgresql_server_host=postgresql_server_host,
        postgresql_server_username="postgres",
    )

    assert (
        postgresql_support.engines._postgresql_url
        == f"postgresql+psycopg2://postgres:{postgresql_server_password}@{postgresql_server_host}/postgres"
    )


def test_postgresql_engine_url_without_server_password(
    postgresql_server_host: str,
) -> None:
    postgresql_support = DatabaseSupport(
        server_software_names=["PostgreSQL"],
        server_password=None,
        postgresql_server_host=postgresql_server_host,
        postgresql_server_username="postgres",
    )

    assert (
        postgresql_support.engines._postgresql_url
        == f"postgresql+psycopg2://postgres@{postgresql_server_host}/postgres"
    )


def test_postgresql_engine_present_for_postgresql_server_software(
    postgresql_support: DatabaseSupport,
) -> None:
    assert "postgresql" in postgresql_support.engines.urls
    assert "postgresql" in postgresql_support.engines.engines
    assert "postgresql" not in postgresql_support.engines.inspectors


def test_mariadb_engine_present_for_mariadb_server_software(
    mariadb_support: DatabaseSupport,
) -> None:
    assert "mysql" in mariadb_support.engines.urls
    assert "mysql" in mariadb_support.engines.engines
    assert "mysql" in mariadb_support.engines.inspectors


def test_postgresql_engine_absent_for_mariadb_server_software(
    mariadb_support: DatabaseSupport,
) -> None:
    assert "postgresql" not in mariadb_support.engines.urls
    assert "postgresql" not in mariadb_support.engines.engines
    assert "postgresql" not in mariadb_support.engines.inspectors


def test_mariadb_engine_absent_for_postgresql_server_software(
    postgresql_support: DatabaseSupport,
) -> None:
    assert "mysql" not in postgresql_support.engines.urls
    assert "mysql" not in postgresql_support.engines.engines
    assert "mysql" not in postgresql_support.engines.inspectors
