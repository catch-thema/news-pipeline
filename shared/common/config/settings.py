from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL 설정
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_db: str = "news_db"

    # MySQL 설정
    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_user: str = "user"
    mysql_password: str = "password"
    mysql_db: str = "stock_crawler"

    server_host: str = "0.0.0.0"
    server_port: int = 8000

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore"
    }


settings = Settings()
