import logging
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from .config import CortexSettings, SnowflakeSettings

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    answer: str
    sql: str | None = None
    data: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


class CortexClient:
    def __init__(
        self,
        snowflake_settings: SnowflakeSettings,
        cortex_settings: CortexSettings,
    ) -> None:
        self.snowflake_settings = snowflake_settings
        self.cortex_settings = cortex_settings
        self._connection: SnowflakeConnection | None = None
        self._session_token: str | None = None

    def _get_connection(self) -> SnowflakeConnection:
        if self._connection is None or self._connection.is_closed():
            self._connection = snowflake.connector.connect(
                account=self.snowflake_settings.account,
                user=self.snowflake_settings.user,
                password=self.snowflake_settings.password,
                warehouse=self.snowflake_settings.warehouse,
                database=self.snowflake_settings.database,
                schema=self.snowflake_settings.schema_,
            )
            self._session_token = None
        return self._connection

    def _get_session_token(self) -> str:
        return self.snowflake_settings.pat

    def _build_agent_url(self) -> str:
        base_url = self.snowflake_settings.base_url
        database = self.snowflake_settings.database
        schema = self.snowflake_settings.schema_
        agent_name = self.cortex_settings.agent_name
        return f"{base_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"

    async def query(
        self,
        question: str,
        thread_id: str | None = None,
        history: list[dict[str, Any]] | None = None,
    ) -> QueryResult:
        if thread_id is None:
            thread_id = str(uuid4())

        if history:
            messages = history + [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ]
        else:
            messages = [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ]

        request_body = {
            "messages": messages,
            "stream": False,
        }

        try:
            token = self._get_session_token()
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            }

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self._build_agent_url(),
                    headers=headers,
                    json=request_body,
                )
                response.raise_for_status()
                data = response.json()
                logger.info("Cortex response: %s", data)
                return self._parse_response(data)

        except httpx.HTTPStatusError as e:
            logger.error("HTTP error from Cortex Agent: %s", e)
            return QueryResult(
                answer="",
                error=f"API error: {e.response.status_code} - {e.response.text}",
            )
        except Exception as e:
            logger.exception("Error querying Cortex Agent")
            return QueryResult(answer="", error=str(e))

    def _parse_response(self, response: dict[str, Any]) -> QueryResult:
        try:
            content = response.get("content", [])
            answer_parts: list[str] = []
            sql_query: str | None = None
            data: list[dict[str, Any]] = []

            for item in content:
                item_type = item.get("type", "")

                if item_type == "text":
                    answer_parts.append(item.get("text", ""))

                elif item_type == "tool_result":
                    tool_result = item.get("tool_result", {})
                    for result_item in tool_result.get("content", []):
                        result_json = result_item.get("json", {})
                        if "sql" in result_json:
                            sql_query = result_json["sql"]
                        result_set = result_json.get("result_set", {})
                        if result_set.get("data"):
                            meta = result_set.get("resultSetMetaData", {})
                            columns = [col["name"] for col in meta.get("rowType", [])]
                            data = [dict(zip(columns, row)) for row in result_set["data"]]

            return QueryResult(
                answer="\n".join(answer_parts).strip(),
                sql=sql_query,
                data=data,
            )

        except Exception as e:
            logger.exception("Error parsing Cortex response")
            return QueryResult(answer="", error=f"Failed to parse response: {e}")

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._session_token = None
