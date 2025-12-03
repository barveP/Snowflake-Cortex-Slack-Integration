import pytest

from cortex_slack_bot.handlers import (
    extract_question,
    format_data_table,
    format_response,
    format_sql_block,
)
from cortex_slack_bot.snowflake_client import QueryResult


class TestFormatSqlBlock:
    def test_formats_sql_with_code_block(self) -> None:
        sql = "SELECT * FROM users"
        result = format_sql_block(sql)
        assert result == "```sql\nSELECT * FROM users\n```"

    def test_handles_multiline_sql(self) -> None:
        sql = "SELECT id, name\nFROM users\nWHERE active = true"
        result = format_sql_block(sql)
        assert "```sql\n" in result
        assert sql in result
        assert result.endswith("\n```")


class TestFormatDataTable:
    def test_formats_simple_table(self) -> None:
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        result = format_data_table(data)
        assert "id" in result
        assert "name" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_empty_data_returns_no_data_message(self) -> None:
        result = format_data_table([])
        assert "_No data returned_" in result

    def test_truncates_to_max_rows(self) -> None:
        data = [{"id": i} for i in range(50)]
        result = format_data_table(data, max_rows=10)
        assert "Showing 10 of 50 rows" in result

    def test_handles_none_values(self) -> None:
        data = [{"id": 1, "name": None}]
        result = format_data_table(data)
        assert "None" in result


class TestFormatResponse:
    def test_formats_error_result(self) -> None:
        result = QueryResult(answer="", error="Connection failed")
        blocks = format_response(result)
        assert len(blocks) == 1
        assert ":x: *Error*" in blocks[0]["text"]["text"]

    def test_formats_successful_result_with_answer(self) -> None:
        result = QueryResult(answer="There are 100 users in the database.")
        blocks = format_response(result)
        assert len(blocks) >= 1
        assert "100 users" in blocks[0]["text"]["text"]

    def test_includes_sql_when_present(self) -> None:
        result = QueryResult(
            answer="Found 5 results.",
            sql="SELECT COUNT(*) FROM users",
        )
        blocks = format_response(result)
        sql_block = next((b for b in blocks if ":mag: *Generated SQL*" in str(b)), None)
        assert sql_block is not None

    def test_includes_data_table_when_present(self) -> None:
        result = QueryResult(
            answer="Here are the top users.",
            data=[{"id": 1, "name": "Alice"}],
        )
        blocks = format_response(result)
        data_block = next((b for b in blocks if ":bar_chart: *Results*" in str(b)), None)
        assert data_block is not None

    def test_truncates_long_answers(self) -> None:
        long_answer = "x" * 5000
        result = QueryResult(answer=long_answer)
        blocks = format_response(result)
        text = blocks[0]["text"]["text"]
        assert len(text) < 5000
        assert text.endswith("...")


class TestExtractQuestion:
    def test_removes_bot_mention(self) -> None:
        text = "<@U1234567> What are the top products?"
        result = extract_question(text, "U1234567")
        assert result == "What are the top products?"

    def test_handles_no_mention(self) -> None:
        text = "What are the top products?"
        result = extract_question(text, "U1234567")
        assert result == "What are the top products?"

    def test_handles_empty_after_mention(self) -> None:
        text = "<@U1234567>"
        result = extract_question(text, "U1234567")
        assert result == ""


class TestQueryResult:
    def test_success_when_no_error(self) -> None:
        result = QueryResult(answer="OK")
        assert result.success is True

    def test_not_success_when_error(self) -> None:
        result = QueryResult(answer="", error="Failed")
        assert result.success is False
