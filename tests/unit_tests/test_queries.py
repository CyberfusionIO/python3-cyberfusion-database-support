import pytest
from sqlalchemy import text

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.queries import Query


@pytest.mark.mariadb
def test_mariadb_query_result_rows(mariadb_support: DatabaseSupport) -> None:
    query = Query(
        engine=mariadb_support.engines.engines["mysql"], query=text("SELECT 1;")
    )

    assert query.result == [(1,)]


@pytest.mark.postgresql
def test_postgresql_query_result_rows(postgresql_support: DatabaseSupport) -> None:
    query = Query(
        engine=postgresql_support.engines.engines["postgresql"],
        query=text("SELECT 1;"),
    )

    assert query.result == [(1,)]


@pytest.mark.mariadb
def test_mariadb_query_result_not_rows(mariadb_support: DatabaseSupport) -> None:
    query = Query(
        engine=mariadb_support.engines.engines["mysql"], query=text("COMMIT;")
    )

    assert query.result == []


@pytest.mark.postgresql
def test_postgresql_query_result_not_rows(postgresql_support: DatabaseSupport) -> None:
    query = Query(
        engine=postgresql_support.engines.engines["postgresql"],
        query=text("COMMIT;"),
    )

    assert query.result == []
