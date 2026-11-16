"""Remember the first moment a sales call is marked closed."""
from datetime import datetime, timezone


def stamp_sale_closed(row, closed: bool | None) -> None:
    row.sale_closed = closed
    if closed is True:
        if getattr(row, "sale_closed_at", None) is None:
            row.sale_closed_at = datetime.now(timezone.utc)
    else:
        row.sale_closed_at = None
