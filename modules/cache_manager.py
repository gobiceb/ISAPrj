"""
Cache Manager Module - Handles Caching and Data Refresh
Prevents API rate limiting and excessive data source hits

Author: Lead Systems Developer
Date: December 2024
"""

import pickle
import json
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Any, Callable, Optional, Dict
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CACHE MANAGER
# ============================================================================

class CacheManager:
    """
    Centralized cache management for data fetching
    
    Features:
    - TTL-based cache expiration
    - Multiple storage backends (file, memory)
    - Automatic refresh logic
    - Cache statistics
    """
    
    def __init__(self,
                 cache_dir: str = ".cache",
                 cache_ttl_minutes: int = 30,
                 backend: str = "file"):
        """
        Initialize cache manager
        
        Args:
            cache_dir: Directory for cache files
            cache_ttl_minutes: Default TTL in minutes
            backend: Storage backend ('file' or 'memory')
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.backend = backend
        self.memory_cache = {}
        self.stats = {
            'hits': 0,
            'misses': 0,
            'expires': 0
        }
        
        logger.info(f"Initialized CacheManager with {backend} backend, TTL: {cache_ttl_minutes}min")
    
    def get_cache_key(self, key: str) -> str:
        """Generate cache key hash"""
        return hashlib.md5(key.encode()).hexdigest()
    
    def get_or_fetch(self,
                     key: str,
                     fetch_func: Callable,
                     force_refresh: bool = False,
                     ttl_minutes: Optional[int] = None) -> Any:
        """
        Get cached data or fetch new data
        
        Args:
            key: Cache key
            fetch_func: Function to fetch data if cache miss
            force_refresh: Force cache refresh
            ttl_minutes: Override default TTL
        
        Returns:
            Cached or fetched data
        """
        try:
            cache_key = self.get_cache_key(key)
            ttl = timedelta(minutes=ttl_minutes) if ttl_minutes else self.cache_ttl
            
            # Check if cache exists and is valid
            if not force_refresh:
                cached_data = self._get_from_cache(cache_key)
                
                if cached_data is not None:
                    data, timestamp = cached_data
                    
                    if datetime.now() - timestamp < ttl:
                        self.stats['hits'] += 1
                        logger.info(f"Cache HIT: {key} (age: {(datetime.now() - timestamp).seconds}s)")
                        return data
                    else:
                        self.stats['expires'] += 1
                        logger.info(f"Cache EXPIRED: {key}")
            
            # Fetch new data
            logger.info(f"Fetching data for: {key}")
            data = fetch_func()
            
            # Store in cache
            self._set_in_cache(cache_key, data, datetime.now())
            self.stats['misses'] += 1
            
            return data
            
        except Exception as e:
            logger.error(f"Error in get_or_fetch: {str(e)}")
            # Try to return stale cache if available
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                logger.warning(f"Returning stale cache for {key} due to fetch error")
                return cached_data[0]
            raise
    
    def _get_from_cache(self, key: str) -> Optional[tuple]:
        """Get data from cache backend"""
        try:
            if self.backend == "memory":
                return self.memory_cache.get(key)
            
            elif self.backend == "file":
                cache_file = self.cache_dir / f"{key}.pkl"
                if cache_file.exists():
                    with open(cache_file, 'rb') as f:
                        return pickle.load(f)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting cache: {str(e)}")
            return None
    
    def _set_in_cache(self, key: str, data: Any, timestamp: datetime) -> bool:
        """Store data in cache backend"""
        try:
            cache_entry = (data, timestamp)
            
            if self.backend == "memory":
                self.memory_cache[key] = cache_entry
                logger.debug(f"Cached to memory: {key}")
                return True
            
            elif self.backend == "file":
                cache_file = self.cache_dir / f"{key}.pkl"
                with open(cache_file, 'wb') as f:
                    pickle.dump(cache_entry, f)
                logger.debug(f"Cached to file: {cache_file}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error setting cache: {str(e)}")
            return False
    
    def clear_cache(self, key: Optional[str] = None) -> bool:
        """
        Clear cache entry or entire cache
        
        Args:
            key: Specific key to clear, or None for all
        
        Returns:
            Success status
        """
        try:
            if key is None:
                # Clear all cache
                if self.backend == "memory":
                    self.memory_cache.clear()
                elif self.backend == "file":
                    for cache_file in self.cache_dir.glob("*.pkl"):
                        cache_file.unlink()
                
                logger.info("Cleared all cache")
                return True
            
            else:
                # Clear specific cache
                cache_key = self.get_cache_key(key)
                
                if self.backend == "memory":
                    self.memory_cache.pop(cache_key, None)
                elif self.backend == "file":
                    cache_file = self.cache_dir / f"{cache_key}.pkl"
                    if cache_file.exists():
                        cache_file.unlink()
                
                logger.info(f"Cleared cache: {key}")
                return True
                
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
            return False
    
    def clear_all(self) -> bool:
        """Alias for clear_cache with no arguments"""
        return self.clear_cache(None)
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total * 100) if total > 0 else 0
        
        stats = {
            **self.stats,
            'total_requests': total,
            'hit_rate': f"{hit_rate:.1f}%"
        }
        
        return stats
    
    def print_stats(self) -> None:
        """Print formatted cache statistics"""
        stats = self.get_stats()
        print(f"""
Cache Statistics:
- Hits: {stats['hits']}
- Misses: {stats['misses']}
- Expirations: {stats['expires']}
- Total Requests: {stats['total_requests']}
- Hit Rate: {stats['hit_rate']}
        """)

# ============================================================================
# DISTRIBUTED CACHE (REDIS OPTIONAL)
# ============================================================================

class DistributedCacheManager(CacheManager):
    """
    Extended cache manager with Redis support for distributed systems
    """
    
    def __init__(self,
                 cache_dir: str = ".cache",
                 cache_ttl_minutes: int = 30,
                 redis_host: Optional[str] = None,
                 redis_port: int = 6379,
                 redis_db: int = 0):
        """
        Initialize distributed cache manager
        
        Args:
            cache_dir: Local cache directory
            cache_ttl_minutes: Default TTL in minutes
            redis_host: Redis server host (None to disable)
            redis_port: Redis server port
            redis_db: Redis database number
        """
        super().__init__(cache_dir, cache_ttl_minutes, "file")
        
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_client = None
        
        if redis_host:
            try:
                import redis
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=redis_db,
                    decode_responses=True
                )
                # Test connection
                self.redis_client.ping()
                logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
            except Exception as e:
                logger.warning(f"Could not connect to Redis: {str(e)}")
                self.redis_client = None
    
    def _get_from_cache(self, key: str) -> Optional[tuple]:
        """Get data from Redis or fallback to file cache"""
        # Try Redis first
        if self.redis_client:
            try:
                cached_json = self.redis_client.get(key)
                if cached_json:
                    cached_dict = json.loads(cached_json)
                    # Reconstruct timestamp
                    timestamp = datetime.fromisoformat(cached_dict['timestamp'])
                    return (cached_dict['data'], timestamp)
            except Exception as e:
                logger.warning(f"Redis retrieval failed: {str(e)}")
        
        # Fallback to file cache
        return super()._get_from_cache(key)
    
    def _set_in_cache(self, key: str, data: Any, timestamp: datetime) -> bool:
        """Store data in Redis and file cache"""
        success = True
        
        # Store in Redis
        if self.redis_client:
            try:
                cache_dict = {
                    'data': data,
                    'timestamp': timestamp.isoformat()
                }
                self.redis_client.setex(
                    key,
                    int(self.cache_ttl.total_seconds()),
                    json.dumps(cache_dict)
                )
                logger.debug(f"Cached to Redis: {key}")
            except Exception as e:
                logger.warning(f"Redis storage failed: {str(e)}")
                success = False
        
        # Also store in file cache
        if not super()._set_in_cache(key, data, timestamp):
            success = False
        
        return success

# ============================================================================
# CACHE WARMER
# ============================================================================

class CacheWarmer:
    """
    Pre-populate cache with data to improve performance
    """
    
    def __init__(self, cache_manager: CacheManager):
        """
        Initialize cache warmer
        
        Args:
            cache_manager: CacheManager instance
        """
        self.cache_manager = cache_manager
        self.warming_schedule = {}
    
    def add_warmup_task(self,
                       key: str,
                       fetch_func: Callable,
                       interval_minutes: int = 60):
        """
        Add a task to warm the cache
        
        Args:
            key: Cache key
            fetch_func: Function to fetch data
            interval_minutes: How often to refresh
        """
        self.warming_schedule[key] = {
            'fetch_func': fetch_func,
            'interval': interval_minutes,
            'last_warmed': None
        }
        
        logger.info(f"Added cache warmup task: {key} (interval: {interval_minutes}min)")
    
    def warm_cache(self):
        """Execute all warmup tasks that are due"""
        now = datetime.now()
        
        for key, task in self.warming_schedule.items():
            last_warmed = task['last_warmed']
            
            if last_warmed is None or \
               (now - last_warmed).total_seconds() > task['interval'] * 60:
                
                try:
                    logger.info(f"Warming cache: {key}")
                    self.cache_manager.get_or_fetch(
                        key,
                        task['fetch_func'],
                        force_refresh=True
                    )
                    task['last_warmed'] = now
                    
                except Exception as e:
                    logger.error(f"Error warming cache {key}: {str(e)}")
    
    def print_schedule(self):
        """Print current warmup schedule"""
        print("Cache Warmup Schedule:")
        for key, task in self.warming_schedule.items():
            print(f"  {key}: every {task['interval']} minutes")

# ============================================================================
# STREAMLIT-SPECIFIC CACHING
# ============================================================================

def streamlit_cache(ttl_minutes: int = 30):
    """
    Decorator for Streamlit @st.cache_data alternative
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            cache_manager = CacheManager(cache_ttl_minutes=ttl_minutes)
            
            # Create cache key from function name and arguments
            key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            
            return cache_manager.get_or_fetch(
                key,
                lambda: func(*args, **kwargs)
            )
        
        return wrapper
    
    return decorator
