import pytest

from cortex_slack_bot.config import CortexSettings, SnowflakeSettings
from cortex_slack_bot.snowflake_client import CortexClient, QueryResult


@pytest.fixture
def snowflake_settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account="test-account.us-east-1",
        user="test_user",
        password="test_password",
        warehouse="TEST_WH",
        database="TEST_DB",
        schema_="TEST_SCHEMA",
    )


@pytest.fixture
def cortex_settings() -> CortexSettings:
    return CortexSettings(agent_name="test_agent")


@pytest.fixture
def cortex_client(
    snowflake_settings: SnowflakeSettings,
    cortex_settings: CortexSettings,
) -> CortexClient:
    return CortexClient(snowflake_settings, cortex_settings)


class TestSnowflakeSettings:
    def test_base_url_generation(self, snowflake_settings: SnowflakeSettings) -> None:
        expected = "https://test-account.us-east-1.snowflakecomputing.com"
        assert snowflake_settings.base_url == expected

    def test_base_url_with_underscores(self) -> None:
        settings = SnowflakeSettings(
            account="test_account_name",
            user="user",
            password="pass",
            warehouse="wh",
            database="db",
        )
        assert "test-account-name" in settings.base_url


class TestCortexClient:
    def test_build_agent_url(self, cortex_client: CortexClient) -> None:
        url = cortex_client._build_agent_url()
        assert "TEST_DB" in url
        assert "TEST_SCHEMA" in url
        assert "test_agent:run" in url

    def test_parse_response_with_text_answer(self, cortex_client: CortexClient) -> None:
        response = {
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "There are 100 users."}],
                }
            ]
        }
        result = cortex_client._parse_response(response)
        assert result.answer == "There are 100 users."
        assert result.sql is None

    def test_parse_response_with_sql_and_data(self, cortex_client: CortexClient) -> None:
        response = {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "Found 2 users."},
                        {
                            "type": "tool_results",
                            "tool_results": [
                                {
                                    "tool_name": "analyst",
                                    "content": {
                                        "sql": "SELECT * FROM users LIMIT 2",
                                        "data": [
                                            {"id": 1, "name": "Alice"},
                                            {"id": 2, "name": "Bob"},
                                        ],
                                    },
                                }
                            ],
                        },
                    ],
                }
            ]
        }
        result = cortex_client._parse_response(response)
        assert result.answer == "Found 2 users."
        assert result.sql == "SELECT * FROM users LIMIT 2"
        assert len(result.data) == 2

    def test_parse_response_empty_messages(self, cortex_client: CortexClient) -> None:
        response = {"messages": []}
        result = cortex_client._parse_response(response)
        assert result.answer == ""


class TestQueryResult:
    def test_success_property(self) -> None:
        assert QueryResult(answer="Success").success is True
        assert QueryResult(answer="", error="Failed").success is False

    def test_default_data_is_empty_list(self) -> None:
        result = QueryResult(answer="OK")
        assert result.data == []
