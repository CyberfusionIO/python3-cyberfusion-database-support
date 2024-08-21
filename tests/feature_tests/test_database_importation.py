import os
from typing import Generator

import pytest
from pytest_mock import MockerFixture  # type: ignore[attr-defined]

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.database_importation import (
    DatabaseImportation,
)
from cyberfusion.DatabaseSupport.databases import Database
from cyberfusion.DatabaseSupport.exceptions import ServerNotSupportedError
from cyberfusion.DatabaseSupport.servers import Server


@pytest.mark.mariadb
def test_mariadb_database_importation_load(
    database_importation,
) -> None:
    database_importation.load()


@pytest.mark.postgresql
def test_postgresql_database_importation_not_supported(
    postgresql_support: DatabaseSupport,
    postgresql_database_created_1: Generator[Database, None, None],
) -> None:
    with pytest.raises(ServerNotSupportedError):
        DatabaseImportation(
            privileged_support=postgresql_support,
            database_name=postgresql_database_created_1.name,
            server_software_name=postgresql_database_created_1.server_software_name,
            source_path="mariadb_testing_1.sql",
        )
