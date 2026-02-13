"""Redis client configuration for DPP."""

import redis
from typing import Optional

# Redis connection settings (should be from config/env in production)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_DECODE_RESPONSES = True  # Return strings instead of bytes


class RedisClient:
    """Singleton Redis client."""

    _instance: Optional[redis.Redis] = None

    @classmethod
    def get_client(cls) -> redis.Redis:
        """
        Get Redis client instance.

        Returns:
            redis.Redis: Redis client
        """
        if cls._instance is None:
            cls._instance = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=REDIS_DECODE_RESPONSES,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset Redis client (for testing)."""
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None


def get_redis() -> redis.Redis:
    """
    Get Redis client for dependency injection.

    Returns:
        redis.Redis: Redis client
    """
    return RedisClient.get_client()
