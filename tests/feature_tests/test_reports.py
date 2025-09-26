from typing import Generator

import pytest
from pytest_mock import MockerFixture

from cyberfusion.DatabaseSupport.database_importation import DatabaseImportation
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import ServerNotSupportedError
from cyberfusion.DatabaseSupport.reports import InnodbReportGenerator
from cyberfusion.DatabaseSupport.servers import Server


@pytest.mark.postgresql
def test_InnodbReportGenerator_generate_not_supported(
    postgresql_server: Generator[Server, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        InnodbReportGenerator.generate(postgresql_server)


@pytest.mark.mariadb
def test_InnodbReportGenerator_generate(
    mocker: MockerFixture,
    mariadb_server: Server,
    mariadb_database_created_1: Generator[Database, None, None],
) -> None:
    mocker.patch(
        "cyberfusion.DatabaseSupport.database_importation.DatabaseImportation.NAME_ACCESS_HOST",
        "%",
    )

    DatabaseImportation(
        privileged_support=mariadb_database_created_1.support,
        server_software_name=mariadb_database_created_1.support.MARIADB_SERVER_SOFTWARE_NAME,
        database_name=mariadb_database_created_1.name,
        source_path="tests/dumps/table_with_random_data.sql",
    ).load()

    report = InnodbReportGenerator.generate(mariadb_server)

    assert report.innodb_buffer_pool_size_bytes >= 0
    assert report.total_innodb_data_length_bytes >= 0

    databases_innodb_data_lengths = report.databases_innodb_data_lengths

    assert len(databases_innodb_data_lengths) == 2

    database_innodb_data_lengths = next(
        d
        for d in databases_innodb_data_lengths
        if d.name == mariadb_database_created_1.name
    )

    assert database_innodb_data_lengths.name == mariadb_database_created_1.name
    assert database_innodb_data_lengths.total_length_bytes == 49152

    assert len(database_innodb_data_lengths.tables_data_lengths) == 1

    table_innodb_data_lengths = database_innodb_data_lengths.tables_data_lengths[0]

    assert table_innodb_data_lengths.name == "sample_data"
    assert table_innodb_data_lengths.total_length_bytes == 49152
    assert table_innodb_data_lengths.data_length_bytes == 16384
    assert table_innodb_data_lengths.index_length_bytes == 32768
