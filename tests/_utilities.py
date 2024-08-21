import secrets
import string


def generate_random_string() -> str:
    length = 8

    return "".join(
        secrets.choice(string.ascii_lowercase) for _ in range(length)
    )


def add_worker_id_to_server_host(server_host: str, worker_id: str) -> str:
    """Add pytest-xdist worker ID to server host.

    At any time, only one test may run on a DBMS. Otherwise, tests would run
    into race conditions (e.g. database A seen by worker A, database A deleted
    by worker B, database A operated on by worker A = error).

    This function ensures that a dedicated DBMS is used, by adding the worker ID
    to the given server host.
    """
    if worker_id == "master":
        return server_host

    return server_host + "-" + worker_id
