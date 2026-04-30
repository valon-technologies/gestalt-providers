GITHUB_API_VERSION = "2022-11-28"
GITHUB_DEFAULT_API_BASE_URL = "https://api.github.com"
GITHUB_DEFAULT_WEB_BASE_URL = "https://github.com"

GITHUB_EVENT_OPERATION = "events.handle"
BOT_COMMIT_FILES_OPERATION = "bot.commitFiles"
BOT_OPEN_PULL_REQUEST_OPERATION = "bot.openPullRequest"
BOT_CREATE_PULL_REQUEST_OPERATION = "bot.createPullRequest"

GITHUB_WORKFLOW_SIGNAL_NAME = "github.app.webhook"

GITHUB_INSTALLATION_SUBJECT_PREFIX = "service_account:github_app_installation:"
GITHUB_REPOSITORY_SUBJECT_SEPARATOR = ":repo:"

DEFAULT_WEBHOOK_EVENTS = (
    "check_run",
    "check_suite",
    "issue_comment",
    "issues",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "workflow_run",
)

DEFAULT_AGENT_SYSTEM_PROMPT = """
You are a GitHub App bot running inside Gestalt.
You are responding to a verified GitHub App webhook, not a user OAuth connection.
Use the available GitHub bot tools to inspect or change GitHub state.
When you create commits or pull requests, use the installation_id and repository
details from the event unless the user instruction says otherwise.
Return a concise final summary of what you did.
""".strip()
MAX_AGENT_USER_PROMPT_CHARS = 12000
MAX_GITHUB_BODY_CHARS = 4096
MAX_GITHUB_TITLE_CHARS = 512
