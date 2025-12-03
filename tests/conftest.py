import pytest


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    env_vars = {
        "SLACK_BOT_TOKEN": "xoxb-test-token",
        "SLACK_APP_TOKEN": "xapp-test-token",
        "SLACK_SIGNING_SECRET": "test-secret",
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_USER": "test-user",
        "SNOWFLAKE_PASSWORD": "test-password",
        "SNOWFLAKE_WAREHOUSE": "test-warehouse",
        "SNOWFLAKE_DATABASE": "test-database",
        "SNOWFLAKE_SCHEMA": "test-schema",
        "CORTEX_AGENT_NAME": "test-agent",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
