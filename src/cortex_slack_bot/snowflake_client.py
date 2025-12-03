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
        if self._session_token is None:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT SYSTEM$GENERATE_JWT_TOKEN()")
            result = cursor.fetchone()
            if result:
                self._session_token = result[0]
            else:
                raise RuntimeError("Failed to generate session token")
            cursor.close()
        return self._session_token

    def _build_agent_url(self) -> str:
        base_url = self.snowflake_settings.base_url
        database = self.snowflake_settings.database
        schema = self.snowflake_settings.schema_
        agent_name = self.cortex_settings.agent_name
        return f"{base_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"

    async def query(self, question: str, thread_id: str | None = None) -> QueryResult:
        if thread_id is None:
            thread_id = str(uuid4())

        request_body = {
            "thread_id": thread_id,
            "parent_message_id": "0",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
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
                return self._parse_response(response.json())

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
            messages = response.get("messages", [])
            answer_parts: list[str] = []
            sql_query: str | None = None
            data: list[dict[str, Any]] = []

            for message in messages:
                if message.get("role") != "assistant":
                    continue

                content = message.get("content", [])
                for item in content:
                    item_type = item.get("type", "")

                    if item_type == "text":
                        answer_parts.append(item.get("text", ""))

                    elif item_type == "tool_results":
                        tool_results = item.get("tool_results", [])
                        for result in tool_results:
                            if result.get("tool_name") == "analyst":
                                result_content = result.get("content", {})
                                sql_query = result_content.get("sql")
                                data = result_content.get("data", [])

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
