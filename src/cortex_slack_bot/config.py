from pydantic_settings import BaseSettings, SettingsConfigDict


class SlackSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SLACK_")

    bot_token: str
    app_token: str
    signing_secret: str


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema_: str = "PUBLIC"

    @property
    def base_url(self) -> str:
        account = self.account.replace("_", "-")
        return f"https://{account}.snowflakecomputing.com"


class CortexSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CORTEX_")

    agent_name: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    slack: SlackSettings = SlackSettings()  # type: ignore[call-arg]
    snowflake: SnowflakeSettings = SnowflakeSettings()  # type: ignore[call-arg]
    cortex: CortexSettings = CortexSettings()  # type: ignore[call-arg]


def get_settings() -> Settings:
    return Settings()
