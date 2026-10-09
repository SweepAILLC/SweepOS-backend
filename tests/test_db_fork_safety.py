"""Forked children (RQ work-horses) must not reuse the parent's pooled DB connections."""
from unittest.mock import patch

from app.db import session


def test_fork_hook_drops_pool_without_closing_parent_sockets():
    with patch.object(session.engine, "dispose") as dispose:
        session._reset_pool_in_forked_child()
    dispose.assert_called_once_with(close=False)
