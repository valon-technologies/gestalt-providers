import gestalt


class ListInput(gestalt.Model):
    limit: int | None = gestalt.field(
        description="Maximum number of records to return per page. Ashby caps this at 100.",
        default=None,
    )
    cursor: str = gestalt.field(
        description="Opaque pagination cursor returned by the previous page.",
        default="",
    )
