class GitHubConfigError(RuntimeError):
    pass


class GitHubAPIError(RuntimeError):
    def __init__(self, status: int, message: str, *, details: str = "") -> None:
        self.status = status
        self.message = message
        self.details = details
        super().__init__(message)


class GitHubAuthorizationError(RuntimeError):
    pass
