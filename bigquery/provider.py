import gestalt

from internals import QueryInput, QueryResult, query_operation


@gestalt.operation(description="Execute a BigQuery SQL query")
def query(input: QueryInput, req: gestalt.Request) -> QueryResult:
    return query_operation(input, req)
