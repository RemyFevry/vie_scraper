from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    AIRTABLE_API_KEY: str
    BASE_ID: str
    TABLE_NAME: str
    EMAIL_ADDRESS: str
    EMAIL_PASSWORD: str
    
    class Config:
        env_file = ".env"

settings = Settings()