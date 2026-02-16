import time
from typing import Optional
import typer
from cyberfusion.DatabaseSupport import DatabaseSupport
from cyberfusion.DatabaseSupport.servers import Server

app = typer.Typer()


@app.command()
def main(
    host: str = typer.Option(..., help="MariaDB host"),
    username: str = typer.Option(..., help="MariaDB username"),
    password: Optional[str] = typer.Option(None, help="MariaDB password"),
) -> None:
    support = DatabaseSupport(
        server_software_names=[DatabaseSupport.MARIADB_SERVER_SOFTWARE_NAME],
        mariadb_server_host=host,
        mariadb_server_username=username,
        server_password=password,
    )

    server = Server(support=support)

    while True:
        if server.get_global_status_variable("wsrep_ready") == "ON":
            break

        time.sleep(1)
