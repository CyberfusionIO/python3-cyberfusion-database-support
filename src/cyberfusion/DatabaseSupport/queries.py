"""Classes for interaction with database queries."""

import sqlalchemy as sa
from sqlalchemy import TextClause


class Query:
    """Abstract representation of database query."""

    def __init__(self, *, engine: sa.engine.base.Engine, query: TextClause) -> None:
        """Set attributes and call functions to handle query."""
        self.engine = engine
        self.query = query

        self._execute()

    def _execute(self) -> None:
        """Execute query."""
        with self.engine.begin() as connection:
            result_proxy = connection.execute(self.query)

            if result_proxy.returns_rows:
                self._result = result_proxy.all()
            else:
                self._result = []

    @property
    def result(self) -> list:
        """Get result."""
        return self._result
