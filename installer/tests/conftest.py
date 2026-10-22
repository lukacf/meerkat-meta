"""Session-wide support for the installer tests."""
import pytest

from owner_access import restore_owner_access


@pytest.fixture(scope="session", autouse=True)
def _session_directories_stay_removable(tmp_path_factory):
    # Rows that withhold write or search on a directory leave it that way, and
    # pytest could not remove such a session later.
    yield
    restore_owner_access(tmp_path_factory.getbasetemp())
