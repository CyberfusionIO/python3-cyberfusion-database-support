import time
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.scripts.wait_wsrep_ready import app

runner = CliRunner()


def test_wait_wsrep_ready(
    mocker: MockerFixture, mariadb_support: DatabaseSupport
) -> None:
    mocker.patch(
        "cyberfusion.DatabaseSupport.servers.Server.get_global_status_variable",
        return_value="ON",
    )

    result = runner.invoke(
        app,
        [
            "--host",
            mariadb_support.mariadb_server_host,
            "--username",
            mariadb_support.mariadb_server_username,
            "--password",
            mariadb_support.server_password,
        ],
    )

    assert result.exit_code == 0, result.stderr


def test_wait_wsrep_ready_polling(
    mocker: MockerFixture, mariadb_support: DatabaseSupport
) -> None:
    get_mock = mocker.patch(
        "cyberfusion.DatabaseSupport.servers.Server.get_global_status_variable",
        side_effect=["OFF", "OFF", "ON"],
    )

    sleep_spy = mocker.spy(time, "sleep")

    result = runner.invoke(
        app,
        [
            "--host",
            mariadb_support.mariadb_server_host,
            "--username",
            mariadb_support.mariadb_server_username,
            "--password",
            mariadb_support.server_password,
        ],
    )

    assert result.exit_code == 0, result.stderr

    assert get_mock.call_count == 3
    assert sleep_spy.call_count == 2

    sleep_spy.assert_called_with(1)
