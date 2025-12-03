# Cortex Slack Bot

> Chat with your data using natural language. Powered by Snowflake Cortex AI Agents.

<!-- Add your architecture diagram here -->
<!-- ![Architecture](docs/images/architecture.png) -->

Transform how your team interacts with data. Ask questions in plain English, get SQL-powered answers in seconds.

## Features

- **Natural Language Queries**: Ask questions like "What were our top products last month?"
- **Automatic SQL Generation**: Cortex AI generates optimized SQL for complex joins and aggregations
- **Real-time Results**: Query results displayed directly in Slack with formatted tables
- **Conversation Context**: Follow-up questions maintain context within threads
- **95% Faster**: Reduce data query time from 10 minutes to 30 seconds

<!-- Add your demo screenshot here -->
<!-- ![Demo](docs/images/demo.png) -->

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│                 │     │                 │     │                         │
│   Slack User    │────▶│   Slack Bot     │────▶│  Snowflake Cortex AI    │
│                 │     │  (Socket Mode)  │     │       Agent             │
│                 │◀────│                 │◀────│                         │
└─────────────────┘     └─────────────────┘     └───────────┬─────────────┘
                                                            │
                                                            ▼
                                                ┌─────────────────────────┐
                                                │                         │
                                                │   Snowflake Warehouse   │
                                                │   (Your Data Tables)    │
                                                │                         │
                                                └─────────────────────────┘
```

## Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/) for dependency management
- Snowflake account with Cortex AI Agents enabled
- Slack workspace with admin access

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/yourusername/cortex-slack-bot.git
cd cortex-slack-bot
poetry install
```

### 2. Create Slack App

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Configure the following:

**OAuth & Permissions** - Add these Bot Token Scopes:
- `app_mentions:read`
- `chat:write`
- `im:history`
- `im:read`
- `im:write`

**Socket Mode**:
- Enable Socket Mode
- Generate an App-Level Token with `connections:write` scope

**Event Subscriptions** - Subscribe to:
- `app_mention`
- `message.im`
- `app_home_opened`

4. Install the app to your workspace

### 3. Set Up Snowflake Cortex Agent

1. In Snowsight, navigate to **AI & ML** → **Agents**
2. Click **Create Agent**
3. Configure your semantic model and data sources
4. Note the agent name for configuration

See [Snowflake Cortex Agents Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents) for detailed setup.

### 4. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
```

### 5. Run the Bot

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

### Example Queries

| Natural Language | What It Does |
|-----------------|--------------|
| "What were our sales last month?" | Time-series aggregation |
| "Top 10 products by revenue" | Ranking with aggregation |
| "Compare Q1 vs Q2 performance" | Period comparison |
| "Show me users who signed up this week" | Filtered date queries |
| "Average order value by region" | Grouped aggregations |

<!-- Add your example output screenshot here -->
<!-- ![Example Output](docs/images/example-output.png) -->

## Development

### Running Tests

```bash
poetry run pytest
```

With coverage:

```bash
poetry run pytest --cov=cortex_slack_bot --cov-report=html
```

### Code Quality

```bash
# Linting
poetry run ruff check .

# Type checking
poetry run mypy src
```


## How It Works

1. **User sends a question** in Slack (mention or DM)
2. **Bot receives the message** via Socket Mode (no public endpoint needed)
3. **Question is sent to Cortex Agent** via REST API
4. **Cortex AI analyzes the question** using your semantic model
5. **SQL is generated and executed** against your Snowflake warehouse
6. **Results are formatted** as Slack blocks with tables
7. **Response is sent** back to the user in the same thread

