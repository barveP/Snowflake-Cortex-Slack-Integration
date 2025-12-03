import asyncio
import logging
import re
from typing import Any

from slack_bolt import App
from slack_bolt.context.say import Say

from .snowflake_client import CortexClient, QueryResult

logger = logging.getLogger(__name__)

MAX_SLACK_MESSAGE_LENGTH = 3000
MAX_TABLE_ROWS = 20


def format_sql_block(sql: str) -> str:
    return f"```sql\n{sql}\n```"


def format_data_table(data: list[dict[str, Any]], max_rows: int = MAX_TABLE_ROWS) -> str:
    if not data:
        return "_No data returned_"

    columns = list(data[0].keys())
    display_data = data[:max_rows]

    widths = {col: len(str(col)) for col in columns}
    for row in display_data:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    max_col_width = 25
    widths = {col: min(w, max_col_width) for col, w in widths.items()}

    header = " | ".join(str(col).ljust(widths[col])[:widths[col]] for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)

    rows: list[str] = []
    for row in display_data:
        formatted_row = " | ".join(
            str(row.get(col, "")).ljust(widths[col])[:widths[col]] for col in columns
        )
        rows.append(formatted_row)

    table = f"```\n{header}\n{separator}\n" + "\n".join(rows) + "\n```"

    if len(data) > max_rows:
        table += f"\n_Showing {max_rows} of {len(data)} rows_"

    return table


def format_response(result: QueryResult) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []

    if result.error:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":x: *Error*\n{result.error}"},
        })
        return blocks

    if result.answer:
        answer_text = result.answer
        if len(answer_text) > MAX_SLACK_MESSAGE_LENGTH:
            answer_text = answer_text[:MAX_SLACK_MESSAGE_LENGTH] + "..."

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": answer_text},
        })

    if result.sql:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":mag: *Generated SQL*\n{format_sql_block(result.sql)}"},
        })

    if result.data:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":bar_chart: *Results*\n{format_data_table(result.data)}"},
        })

    return blocks


def extract_question(text: str, bot_user_id: str) -> str:
    pattern = rf"<@{bot_user_id}>"
    return re.sub(pattern, "", text).strip()


def register_handlers(app: App, cortex_client: CortexClient) -> None:

    @app.event("app_mention")
    def handle_mention(event: dict[str, Any], say: Say, context: dict[str, Any]) -> None:
        user = event.get("user", "")
        text = event.get("text", "")
        thread_ts = event.get("thread_ts") or event.get("ts")
        bot_user_id = context.get("bot_user_id", "")

        question = extract_question(text, bot_user_id)
        if not question:
            say(text="Please ask me a question about your data!", thread_ts=thread_ts)
            return

        say(text=":hourglass_flowing_sand: Analyzing your question...", thread_ts=thread_ts)

        try:
            result = asyncio.get_event_loop().run_until_complete(
                cortex_client.query(question, thread_id=thread_ts)
            )
        except RuntimeError:
            result = asyncio.run(cortex_client.query(question, thread_id=thread_ts))

        blocks = format_response(result)
        fallback_text = result.answer if result.success else f"Error: {result.error}"

        say(text=fallback_text, blocks=blocks, thread_ts=thread_ts)
        logger.info("Processed query from user %s: %s", user, question[:50])

    @app.event("message")
    def handle_message(event: dict[str, Any], say: Say, context: dict[str, Any]) -> None:
        channel_type = event.get("channel_type", "")
        if channel_type != "im":
            return

        if event.get("bot_id"):
            return

        text = event.get("text", "")
        thread_ts = event.get("thread_ts") or event.get("ts")

        if not text.strip():
            return

        say(text=":hourglass_flowing_sand: Analyzing your question...", thread_ts=thread_ts)

        try:
            result = asyncio.get_event_loop().run_until_complete(
                cortex_client.query(text, thread_id=thread_ts)
            )
        except RuntimeError:
            result = asyncio.run(cortex_client.query(text, thread_id=thread_ts))

        blocks = format_response(result)
        fallback_text = result.answer if result.success else f"Error: {result.error}"

        say(text=fallback_text, blocks=blocks, thread_ts=thread_ts)

    @app.event("app_home_opened")
    def handle_app_home(event: dict[str, Any], client: Any) -> None:
        user_id = event.get("user", "")

        home_blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Welcome to Cortex Data Assistant"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "I help you query your Snowflake data using natural language. No SQL knowledge required!",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*How to use me:*"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        ":one: *Mention me* in any channel: `@CortexBot What were our sales last month?`\n"
                        ":two: *DM me* directly with your question\n"
                        ":three: I'll generate the SQL, run it, and show you the results!"
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Example questions:*"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "- _What are the top 10 products by revenue?_\n"
                        "- _Show me the sales trend for the last 6 months_\n"
                        "- _How many active users do we have by region?_\n"
                        "- _Compare Q1 vs Q2 performance_"
                    ),
                },
            },
        ]

        client.views_publish(
            user_id=user_id,
            view={"type": "home", "blocks": home_blocks},
        )
