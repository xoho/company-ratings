import os
from pydantic import BaseModel


class Config(BaseModel):
    APP_NAME: str = os.getenv("APP_NAME", "starter-app")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "super-secret-key")
    ROOT_LOG_LEVEL: str = os.getenv("ROOT_LOG_LEVEL", "INFO")
    STATIC_FOLDER: str = os.getenv("STATIC_FOLDER", os.path.join(os.path.dirname(__file__), "assets"))
    KEYCLOAK_URL: str = os.getenv("KEYCLOAK_URL", "http://localhost:8080/auth/realms/master")
    KEYCLOAK_CLIENT_ID: str = os.getenv("KEYCLOAK_CLIENT_ID", "starter-app")
    KEYCLOAK_CLIENT_SECRET: str = os.getenv("KEYCLOAK_CLIENT_SECRET", "starter-app")
    KEYCLOAK_CLIENT_SCOPE: str = os.getenv("KEYCLOAK_CLIENT_SCOPE", "openid profile email")
    SQLALCHEMY_DATABASE_URI: str = os.getenv("SQLALCHEMY_DATABASE_URI", "sqlite:///app.db")
    RATING_CHANGE_ON_FIRST_LOGIN: int = int(os.getenv("RATING_CHANGE_ON_FIRST_LOGIN", "100"))
    NEGATIVE_USER_SAVE_FOLDER: str = os.getenv(
        "NEGATIVE_USER_SAVE_FOLDER", os.path.join(os.path.dirname(__file__), "assets", "negative_user")
    )
    ANTISOCIAL_CREDIT_LIST_URL: str = os.getenv("ANTISOCIAL_CREDIT_LIST_URL", "/handles.html")
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "super-secret-jwt-key")
    DEFAULT_PER_PAGE: int = int(os.getenv("DEFAULT_PER_PAGE", "25"))
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "")
    AWS_S3_BUCKET_NAME: str = os.getenv("AWS_S3_BUCKET_NAME", "")
    AWS_S3_BASE_KEY: str = os.getenv("AWS_S3_BASE_KEY", "")
    METADATA_SOURCE_FOLDER: str = os.getenv(
        "METADATA_SOURCE_FOLDER", os.path.join(os.path.dirname(__file__), "metadata")
    )
    CACHE_FOLDER = os.getenv("CACHE_FOLDER", os.path.join(os.path.dirname(__file__), "cache"))


config = Config()

print("S3 bucket: ", config.AWS_S3_BUCKET_NAME)

for key in ["NEGATIVE_USER_SAVE_FOLDER", "STATIC_FOLDER", "METADATA_SOURCE_FOLDER", "CACHE_FOLDER"]:
    if not os.path.exists(getattr(config, key)):
        os.makedirs(getattr(config, key))

if __name__ == "__main__":
    from random import SystemRandom

    for key in [x for x in config.dict().keys()]:
        if key == "SECRET_KEY":
            print(
                f"export {key}='{''.join([SystemRandom().choice('abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)') for i in range(50)])}'"
            )
        else:
            print(f"export {key}={getattr(config, key)}")
