import pytest
from sqlalchemy.engine import ResultProxy

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.queries import Query


@pytest.mark.mariadb
def test_mariadb_query_result(mariadb_support: DatabaseSupport) -> None:
    query = Query(
        engine=mariadb_support.engines.engines["mysql"], query="SELECT 1;"
    )

    assert isinstance(query.result, ResultProxy)


@pytest.mark.postgresql
def test_postgresql_query_result(postgresql_support: DatabaseSupport) -> None:
    query = Query(
        engine=postgresql_support.engines.engines["postgresql"],
        query="SELECT 1;",
    )

    assert isinstance(query.result, ResultProxy)
