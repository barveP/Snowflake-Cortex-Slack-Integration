# Cortex Slack Bot

> Chat with your data using natural language. Powered by Snowflake Cortex AI Agents.

Transform how your team interacts with data. Ask questions in plain English, get SQL-powered answers in seconds — with the generated SQL and full results attached as files.

## Architecture

![Architecture](assets/architecture.png)

1. Users mention `@CortexBot` in a channel or DM the bot directly
2. Slack delivers the message over **WebSocket** (Socket Mode — no public URL needed)
3. The bot extracts the question, retrieves thread history for context, and sends a **POST** request to the Cortex Agent REST API at `/api/v2/databases/{db}/schemas/{schema}/agents/{agent}:run`
4. The Cortex Agent uses **Cortex Analyst** (and optionally Cortex Search or custom tools) to generate SQL, execute it against your Snowflake warehouse, and return the results
5. The bot parses the response and sends back: a text summary in the thread, a `.sql` file with the generated query, and a `.csv` file with the full results

## Features

- **Natural Language Queries** — Ask questions like "What were our top products last month?"
- **Automatic SQL Generation** — Cortex AI generates optimized SQL for complex joins, CTEs, and window functions
- **File Attachments** — Generated SQL attached as `.sql`, query results as `.csv` — no more truncated tables
- **Conversation Memory** — Follow-up questions within a Slack thread maintain full context (up to 20 messages)
- **Dual Interface** — Works via @mentions in channels and direct messages
- **App Home** — Welcome tab with usage instructions and example queries

## Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/) for dependency management
- Snowflake account with Cortex AI Agents enabled
- Snowflake Programmatic Access Token (PAT)
- Slack workspace with admin access

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/yourusername/cortex-slack-bot.git
cd cortex-slack-bot
python3 -m venv .venv
source .venv/bin/activate
poetry install
```

### 2. Create Slack App

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Configure the following:

**OAuth & Permissions** — Add these Bot Token Scopes:
- `app_mentions:read`
- `chat:write`
- `files:write`
- `im:history`
- `im:read`
- `im:write`

**Socket Mode**:
- Enable Socket Mode
- Generate an App-Level Token with `connections:write` scope

**Event Subscriptions** — Subscribe to:
- `app_mention`
- `message.im`
- `app_home_opened`

**App Home**:
- Enable the **Messages Tab** (so users can DM the bot)

4. Install the app to your workspace

### 3. Set Up Snowflake Cortex Agent

1. In Snowsight, navigate to **AI & ML** → **Agents**
2. Click **Create Agent**
3. Configure your semantic model and data sources
4. Note the agent name, database, and schema

See [Snowflake Cortex Agents Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents) for detailed setup.

### 4. Generate a Programmatic Access Token (PAT)

1. In Snowsight, click your user menu (bottom-left)
2. Go to **Programmatic Access Tokens**
3. Generate a new token and copy it

> **Note:** Snowflake REST APIs do not support username/password auth directly. PAT is the simplest auth method — no key pairs or OAuth setup required.

### 5. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Slack
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token
SLACK_SIGNING_SECRET=your-signing-secret

# Snowflake
SNOWFLAKE_ACCOUNT=ORG-ACCOUNT
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_PAT=your-programmatic-access-token
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema

# Cortex Agent
CORTEX_AGENT_NAME=your-agent-name
```

### 6. Network Policy (if required)

If your Snowflake account has network policies enabled, allowlist your IP:

```sql
CREATE OR REPLACE NETWORK RULE allow_my_ip
    MODE = INGRESS TYPE = IPV4 VALUE_LIST = ('YOUR_IP/32');

CREATE OR REPLACE NETWORK POLICY my_policy
    ALLOWED_NETWORK_RULE_LIST = ('allow_my_ip');

ALTER USER your_user SET NETWORK_POLICY = 'my_policy';
```

### 7. Run the Bot

```bash
poetry run cortex-bot
```

Or with Python directly:

```bash
poetry run python -m cortex_slack_bot.app
```

## Usage

### Mention the Bot

In any channel where the bot is invited:

```
@CortexBot What were our total sales last quarter?
```

### Direct Message

Send a DM to the bot:

```
Show me the top 10 customers by revenue
```

### Follow-up Questions

Ask follow-ups in the same thread and the bot keeps context:

```
@CortexBot Show me total sales by category
  → (bot responds with breakdown)
@CortexBot Break that down by month
  → (bot understands "that" = sales by category)
```

### Example Queries

| Natural Language | What It Does |
|-----------------|--------------|
| "What were our sales last month?" | Time-series aggregation |
| "Top 10 products by revenue" | Ranking with aggregation |
| "Compare Q1 vs Q2 performance" | Period comparison |
| "Show me users who signed up this week" | Filtered date queries |
| "Average order value by region" | Grouped aggregations |

## How It Works

1. **User sends a question** in Slack (mention or DM)
2. **Bot receives the message** via WebSocket (Socket Mode)
3. **Thread history is retrieved** from in-memory store for conversation context
4. **Question + history sent to Cortex Agent** via REST API with PAT auth
5. **Cortex AI generates and executes SQL** against your Snowflake warehouse
6. **Bot parses the response** — extracts the text answer, SQL query, and result data
7. **Text answer posted** to Slack thread
8. **SQL and results attached** as `.sql` and `.csv` files in the thread
9. **Exchange stored** in thread history for future follow-ups

## Project Structure

```
src/cortex_slack_bot/
├── app.py              # Entry point — creates app, starts Socket Mode
├── config.py           # Pydantic settings — loads and validates .env
├── snowflake_client.py # Cortex Agent HTTP client and response parser
└── handlers.py         # Slack event handlers, formatting, file uploads, thread memory
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Slack SDK | `slack-bolt` with Socket Mode |
| HTTP Client | `httpx` (async) |
| Config | `pydantic-settings` with `.env` |
| Auth | Snowflake PAT (Bearer token) |
| Language | Python 3.10+ |
| Build | Poetry |
