class GitHubConfigError(RuntimeError):
    pass


class GitHubAPIError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(message)


class GitHubAuthorizationError(RuntimeError):
    pass
