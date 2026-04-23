import os

def get_env(key: str) -> str:
    """Get environment variable value by key."""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"{key} is required")
    return value