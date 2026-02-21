import asyncio
import csv
import io
import logging
import re
from collections import defaultdict
from typing import Any

from slack_bolt import App
from slack_bolt.context.say import Say

from .snowflake_client import CortexClient, QueryResult

logger = logging.getLogger(__name__)

MAX_BLOCK_TEXT_LENGTH = 2900
MAX_HISTORY_MESSAGES = 20

thread_history: dict[str, list[dict[str, Any]]] = defaultdict(list)


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
        if len(answer_text) > MAX_BLOCK_TEXT_LENGTH:
            answer_text = answer_text[:MAX_BLOCK_TEXT_LENGTH] + "..."
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": answer_text},
        })

    file_notes: list[str] = []
    if result.sql:
        file_notes.append(":mag: SQL query attached")
    if result.data:
        file_notes.append(f":bar_chart: Results attached ({len(result.data)} rows)")
    if file_notes:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " | ".join(file_notes)}],
        })

    return blocks


def upload_files(client: Any, channel: str, thread_ts: str, result: QueryResult) -> None:
    if not result.sql and not result.data:
        return

    uploads: list[dict[str, Any]] = []

    if result.sql:
        uploads.append({
            "content": result.sql,
            "filename": "query.sql",
            "title": "Generated SQL",
        })

    if result.data:
        buf = io.StringIO()
        columns = list(result.data[0].keys())
        writer = csv.DictWriter(buf, fieldnames=columns)
        writer.writeheader()
        writer.writerows(result.data)
        uploads.append({
            "content": buf.getvalue(),
            "filename": "results.csv",
            "title": "Query Results",
        })

    try:
        client.files_upload_v2(
            file_uploads=uploads,
            channel=channel,
            thread_ts=thread_ts,
        )
    except Exception as e:
        logger.exception("Failed to upload files: %s", e)


def extract_question(text: str, bot_user_id: str) -> str:
    pattern = rf"<@{bot_user_id}>"
    return re.sub(pattern, "", text).strip()


def get_history(thread_ts: str) -> list[dict[str, Any]]:
    return thread_history[thread_ts][-MAX_HISTORY_MESSAGES:]


def store_exchange(thread_ts: str, question: str, answer: str) -> None:
    thread_history[thread_ts].append(
        {"role": "user", "content": [{"type": "text", "text": question}]}
    )
    thread_history[thread_ts].append(
        {"role": "assistant", "content": [{"type": "text", "text": answer}]}
    )


async def run_cortex_query(
    cortex_client: CortexClient,
    question: str,
    thread_id: str | None,
    history: list[dict[str, Any]] | None = None,
) -> QueryResult:
    try:
        return await cortex_client.query(question, thread_id=thread_id, history=history)
    except Exception as e:
        logger.exception("Error running Cortex query")
        return QueryResult(answer="", error=str(e))


def run_query_sync(
    cortex_client: CortexClient,
    question: str,
    thread_id: str | None,
    history: list[dict[str, Any]] | None = None,
) -> QueryResult:
    try:
        result = asyncio.get_event_loop().run_until_complete(
            run_cortex_query(cortex_client, question, thread_id, history)
        )
    except RuntimeError:
        result = asyncio.run(run_cortex_query(cortex_client, question, thread_id, history))
    return result


def register_handlers(app: App, cortex_client: CortexClient) -> None:

    @app.event("app_mention")
    def handle_mention(event: dict[str, Any], say: Say, context: dict[str, Any], client: Any) -> None:
        user = event.get("user", "")
        text = event.get("text", "")
        channel = event.get("channel", "")
        thread_ts = event.get("thread_ts") or event.get("ts")
        bot_user_id = context.get("bot_user_id", "")

        question = extract_question(text, bot_user_id)
        if not question:
            say(text="Please ask me a question about your data!", thread_ts=thread_ts)
            return

        say(text=":hourglass_flowing_sand: Analyzing your question...", thread_ts=thread_ts)

        history = get_history(thread_ts)
        result = run_query_sync(cortex_client, question, thread_ts, history)
        blocks = format_response(result)
        fallback_text = (result.answer or "No response from agent.") if result.success else f"Error: {result.error}"

        say(text=fallback_text, blocks=blocks, thread_ts=thread_ts)
        upload_files(client, channel, thread_ts, result)
        store_exchange(thread_ts, question, result.answer or "No response from agent.")
        logger.info("Processed query from user %s: %s", user, question[:50])

    @app.event("message")
    def handle_message(event: dict[str, Any], say: Say, context: dict[str, Any], client: Any) -> None:
        channel_type = event.get("channel_type", "")
        if channel_type != "im":
            return

        if event.get("bot_id"):
            return

        text = event.get("text", "")
        channel = event.get("channel", "")
        thread_ts = event.get("thread_ts") or event.get("ts")

        if not text.strip():
            return

        say(text=":hourglass_flowing_sand: Analyzing your question...", thread_ts=thread_ts)

        history = get_history(thread_ts)
        result = run_query_sync(cortex_client, text, thread_ts, history)
        blocks = format_response(result)
        fallback_text = (result.answer or "No response from agent.") if result.success else f"Error: {result.error}"

        say(text=fallback_text, blocks=blocks, thread_ts=thread_ts)
        upload_files(client, channel, thread_ts, result)
        store_exchange(thread_ts, text, result.answer or "No response from agent.")

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
