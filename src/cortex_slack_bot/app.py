import logging
import sys

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .config import get_settings
from .handlers import register_handlers
from .snowflake_client import CortexClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_app() -> tuple[App, CortexClient]:
    settings = get_settings()

    app = App(
        token=settings.slack.bot_token,
        signing_secret=settings.slack.signing_secret,
    )

    cortex_client = CortexClient(
        snowflake_settings=settings.snowflake,
        cortex_settings=settings.cortex,
    )

    register_handlers(app, cortex_client)

    logger.info("Cortex Slack Bot initialized")
    return app, cortex_client


def main() -> None:
    try:
        settings = get_settings()
        app, cortex_client = create_app()

        handler = SocketModeHandler(app, settings.slack.app_token)
        logger.info("Starting Cortex Slack Bot...")
        handler.start()
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.exception("Failed to start bot: %s", e)
        sys.exit(1)
    finally:
        if "cortex_client" in locals():
            cortex_client.close()


if __name__ == "__main__":
    main()
