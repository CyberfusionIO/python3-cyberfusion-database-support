import os
import pwd
from typing import Generator

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture  # type: ignore[attr-defined]
from sqlalchemy import MetaData

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import ServerNotSupportedError
from cyberfusion.DatabaseSupport.servers import Server
from cyberfusion.DatabaseSupport.tables import Table
from cyberfusion.DatabaseSupport.utilities import generate_random_string


@pytest.mark.mariadb
def test_database_size_no_result(
    mocker: MockerFixture,
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    mocker.patch(
        "cyberfusion.DatabaseSupport.queries.Query.result",
        new=mocker.PropertyMock(
            return_value=[
                (8192,),
                (8192,),
                (0,),
                (8192,),
                (8192,),
                (16384,),
                (8192,),
                (8192,),
                (180224,),
                (16384,),
                (16384,),
                (16384,),
                (8192,),
                (16384,),
                (1531904,),
                (8192,),
                (16384,),
                (16384,),
                (8192,),
                (None,),
                (16384,),
                (8192,),
                (16384,),
                (16384,),
                (8192,),
                (8192,),
                (8192,),
                (0,),
                (8192,),
                (16384,),
                (8192,),
            ]
        ),
    )

    assert mariadb_database_created_1.size >= 0


@pytest.mark.mariadb
def test_mariadb_database_not_drop_when_not_exists(
    mariadb_database: Generator[Database, None, None],
) -> None:
    assert not mariadb_database.drop()


@pytest.mark.mariadb
def test_mariadb_database_drop_when_exists(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    assert mariadb_database_created_1.drop()


@pytest.mark.mariadb
def test_mariadb_database_create_when_not_exists(
    mariadb_database: Generator[Database, None, None],
) -> None:
    assert mariadb_database.create()


@pytest.mark.mariadb
def test_mariadb_database_not_create_when_exists(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    assert not mariadb_database_created_1.create()


@pytest.mark.mariadb
def test_mariadb_database_exists(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    assert mariadb_database_created_1.exists


@pytest.mark.mariadb
def test_mariadb_database_not_exists(
    mariadb_server: Server, mariadb_database: Generator[Database, None, None]
) -> None:
    assert not mariadb_database.exists


@pytest.mark.mariadb
def test_mariadb_database_url(
    mariadb_server_password: str,
    mariadb_database: Generator[Database, None, None],
    mariadb_server_host: str,
) -> None:
    assert (
        mariadb_database.url
        == f"mysql+pymysql://root:{mariadb_server_password}@{mariadb_server_host}/{mariadb_database.name}"
    )


@pytest.mark.mariadb
def test_mariadb_mysql_credentials_config_file_without_server_password(
    mariadb_server_host: str,
) -> None:
    USERNAME = generate_random_string()

    support = DatabaseSupport(
        server_software_names=["MariaDB"],
        # server_password=mariadb_server_password,
        server_password=None,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username=USERNAME,
    )
    server = Server(support=support)
    database = Database(
        support=server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    with open(database._mysql_credentials_config_file, "r") as f:
        assert (
            f.read() == f"[client]\nhost = {mariadb_server_host}\nuser = {USERNAME}\n\n"
        )


@pytest.mark.mariadb
def test_mariadb_mysql_credentials_config_file_with_server_password(
    mocker: MockerFixture,
    mariadb_server_password: str,
    mariadb_server_host: str,
) -> None:
    support = DatabaseSupport(
        server_software_names=["MariaDB"],
        server_password=mariadb_server_password,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username="root",
    )
    server = Server(support=support)
    database = Database(
        support=server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    with open(database._mysql_credentials_config_file, "r") as f:
        assert (
            f.read()
            == f"[client]\nhost = {mariadb_server_host}\nuser = root\npassword = {mariadb_server_password}\n\n"
        )


@pytest.mark.mariadb
def test_mariadb_mysql_credentials_config_file_socket(
    monkeypatch: MonkeyPatch,
    mariadb_server_password: str,
    mariadb_server_host: str,
) -> None:
    # Manually initialise instead of using database fixture, as monkeypatch must
    # not be done before accessing '_mysql_credentials_config_file'

    support = DatabaseSupport(
        server_software_names=["MariaDB"],
        server_password=mariadb_server_password,
        mariadb_server_host=mariadb_server_host,
        mariadb_server_username="root",
    )
    server = Server(support=support)
    database = Database(
        support=server.support,
        name=generate_random_string(),
        server_software_name="MariaDB",
    )

    monkeypatch.setattr(
        support,
        "mariadb_server_host",
        "/run/mysqld/mysql.sock",
        raising=True,
    )

    assert (
        "socket = /run/mysqld/mysql.sock"
        in open(database._mysql_credentials_config_file, "r").read().splitlines()
    )


@pytest.mark.mariadb
def test_mariadb_database_size(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    assert mariadb_database_created_1.size >= 0


@pytest.mark.mariadb
def test_mariadb_database_export_file_name(
    mariadb_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    _dump_file, _ = mariadb_database_created_1.export(root_directory=dump_directory)

    assert _dump_file.startswith(
        os.path.join(dump_directory, mariadb_database_created_1.name)
    )
    assert _dump_file.endswith(".sql")


@pytest.mark.mariadb
def test_mariadb_database_export_md5_hash(
    mariadb_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    _, md5_hash = mariadb_database_created_1.export(root_directory=dump_directory)

    assert "==" in md5_hash


@pytest.mark.mariadb
def test_mariadb_database_export_contents(
    mariadb_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    _dump_file, _ = mariadb_database_created_1.export(root_directory=dump_directory)

    with open(_dump_file, "r") as f:
        contents = f.read()

    assert "Dump completed on" in contents


@pytest.mark.mariadb
def test_mariadb_database_export_no_exclude_tables(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_table_created_1: Generator[Table, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    _dump_file, _ = mariadb_database_created_1.export(
        root_directory=dump_directory, exclude_tables=[]
    )

    with open(_dump_file, "r") as f:
        contents = f.read().splitlines()

    assert f"DROP TABLE IF EXISTS `{mariadb_table_created_1.name}`;" in contents  # Drop
    assert f"CREATE TABLE `{mariadb_table_created_1.name}` (" in contents  # Create
    assert (
        f"-- Dumping data for table `{mariadb_table_created_1.name}`" in contents
    )  # Modify


@pytest.mark.mariadb
def test_mariadb_database_export_exclude_tables(
    mariadb_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    _dump_file, _ = mariadb_database_created_1.export(
        root_directory=dump_directory, exclude_tables=[mariadb_table_created_1]
    )

    with open(_dump_file, "r") as f:
        contents = f.read().splitlines()

    assert (
        f"DROP TABLE IF EXISTS `{mariadb_table_created_1.name}`;" not in contents
    )  # Drop
    assert f"CREATE TABLE `{mariadb_table_created_1.name}` (" not in contents  # Create
    assert (
        f"-- Dumping data for table `{mariadb_table_created_1.name}`" not in contents
    )  # Modify


@pytest.mark.mariadb
def test_mariadb_database_export_chown(
    mocker: MockerFixture,
    mariadb_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    spy_rename = mocker.spy(os, "chown")

    passwd = pwd.getpwnam("nobody")

    try:
        mariadb_database_created_1.export(
            root_directory=dump_directory, chown_username="nobody"
        )
    except PermissionError:
        # When running locally, the user may not be allowed to chown. Because of
        # this, this test doesn't actually check the permissions of the file, but
        # what chown was called with. Therefore, this may fail.

        pass

    spy_rename.assert_called_once_with(mocker.ANY, passwd.pw_uid, passwd.pw_gid)

    # stat = os.stat(_dump_file)
    # passwd = pwd.getpwnam("nobody")
    #
    # assert stat.st_uid == passwd.pw_uid
    # assert stat.st_gid == passwd.pw_gid


@pytest.mark.mariadb
def test_mariadb_database_load(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    with open("mariadb_testing_1.sql", "r") as f:
        mariadb_database_created_1.load(f)


@pytest.mark.postgresql
def test_postgresql_database_not_drop_when_not_exists(
    postgresql_database: Generator[Database, None, None],
) -> None:
    assert not postgresql_database.drop()


@pytest.mark.postgresql
def test_postgresql_database_drop_when_exists(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    assert postgresql_database_created_1.drop()


@pytest.mark.postgresql
def test_postgresql_database_create_when_not_exists(
    postgresql_database: Generator[Database, None, None],
) -> None:
    assert postgresql_database.create()


@pytest.mark.postgresql
def test_postgresql_database_not_create_when_exists(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    assert not postgresql_database_created_1.create()


@pytest.mark.postgresql
def test_postgresql_database_exists(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    assert postgresql_database_created_1.exists


@pytest.mark.postgresql
def test_postgresql_database_not_exists(
    postgresql_server: Server,
    postgresql_database: Generator[Database, None, None],
) -> None:
    assert not postgresql_database.exists


@pytest.mark.postgresql
def test_postgresql_database_url(
    postgresql_server_password: str,
    postgresql_database: Generator[Database, None, None],
    postgresql_server_host: str,
) -> None:
    assert (
        postgresql_database.url
        == f"postgresql+psycopg2://postgres:{postgresql_server_password}@{postgresql_server_host}/{postgresql_database.name}"
    )


@pytest.mark.postgresql
def test_postgresql_database_size(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    assert postgresql_database_created_1.size >= 0


@pytest.mark.postgresql
def test_postgresql_database_export_not_supported(
    postgresql_database_created_1: Generator[Database, None, None],
    dump_directory: Generator[str, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        postgresql_database_created_1.export(root_directory=dump_directory)


@pytest.mark.postgresql
def test_postgresql_database_load_not_supported(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        with open("mariadb_testing_1.sql", "r") as f:
            postgresql_database_created_1.load(f)


@pytest.mark.postgresql
def test_postgresql_database_metadata(
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    assert isinstance(postgresql_database_created_1.metadata, MetaData)

    assert (
        postgresql_database_created_1.metadata.schema
        == postgresql_database_created_1.name
    )


@pytest.mark.mariadb
def test_mariadb_database_metadata(
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    assert isinstance(mariadb_database_created_1.metadata, MetaData)

    assert mariadb_database_created_1.metadata.schema == mariadb_database_created_1.name


@pytest.mark.mariadb
def test_mariadb_tables(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_table_created_1: Generator[Table, None, None],
) -> None:
    assert len(mariadb_database_created_1.tables) == 1

    assert mariadb_database_created_1.tables[0].name == mariadb_table_created_1.name
    assert (
        mariadb_database_created_1.tables[0].database.name
        == mariadb_database_created_1.name
    )


@pytest.mark.postgresql
def test_postgresql_tables(
    postgresql_database_created_1: Generator[Database, None, None],
    postgresql_table_created_1: Generator[Table, None, None],
) -> None:
    assert len(postgresql_database_created_1.tables) == 1

    assert (
        postgresql_database_created_1.tables[0].name == postgresql_table_created_1.name
    )
    assert (
        postgresql_database_created_1.tables[0].database.name
        == postgresql_database_created_1.name
    )


@pytest.mark.postgresql
def test_postgresql_database_compare_not_supported(
    postgresql_database_created_1: Generator[Database, None, None],
    postgresql_database_created_2: Generator[Database, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        postgresql_database_created_1.compare(
            right_database=postgresql_database_created_2
        )


@pytest.mark.mariadb
def test_mariadb_database_compare(
    mariadb_database_created_1: Generator[Database, None, None],
    mariadb_database_created_2: Generator[Database, None, None],
) -> None:
    with open("mariadb_testing_1.sql", "r") as f:
        mariadb_database_created_1.load(f)

    with open("mariadb_testing_2.sql", "r") as f:
        mariadb_database_created_2.load(f)

    (
        present_in_left_and_right,
        present_in_only_left,
        present_in_only_right,
    ) = mariadb_database_created_1.compare(right_database=mariadb_database_created_2)

    assert present_in_left_and_right == {
        "table_in_1_and_2_not_identical": False,
        "table_in_1_and_2_identical": True,
    }
    assert present_in_only_left == ["table_only_in_1"]
    assert present_in_only_right == ["table_only_in_2"]
