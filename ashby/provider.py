import gestalt

from internals import ListInput, ListResult, list_operation


@gestalt.operation(
    id="application.list",
    method="POST",
    description="List applications with working Ashby cursor pagination metadata.",
)
def application_list(
    input: ListInput, req: gestalt.Request
) -> ListResult:
    return list_operation("application.list", input, req)


@gestalt.operation(
    id="offer.list",
    method="POST",
    description="List offers with working Ashby cursor pagination metadata.",
)
def offer_list(input: ListInput, req: gestalt.Request) -> ListResult:
    return list_operation("offer.list", input, req)
