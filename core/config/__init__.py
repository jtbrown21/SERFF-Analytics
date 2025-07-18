"""
Centralized configuration management for the CORE platform.

This module consolidates all configuration scattered across the codebase
into a single, environment-aware configuration system.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    duckdb_path: str = "core/data/sources/insurance_filings.db"
    airtable_base_id: str = ""
    airtable_table_name: str = "Rate Filings"
    airtable_api_key: str = ""
    
    def __post_init__(self):
        """Load values from environment variables."""
        self.duckdb_path = os.getenv("DATABASE_PATH", self.duckdb_path)
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID", self.airtable_base_id)
        self.airtable_table_name = os.getenv("AIRTABLE_TABLE_NAME", self.airtable_table_name)
        self.airtable_api_key = os.getenv("AIRTABLE_API_KEY", self.airtable_api_key)

@dataclass
class EmailConfig:
    """Email service configuration."""
    postmark_token: str = ""
    sender_email: str = ""
    sender_name: str = "Insurance Analytics"
    
    def __post_init__(self):
        """Load values from environment variables."""
        self.postmark_token = os.getenv("POSTMARK_TOKEN", self.postmark_token)
        self.sender_email = os.getenv("SENDER_EMAIL", self.sender_email)
        self.sender_name = os.getenv("SENDER_NAME", self.sender_name)

@dataclass
class ReportingConfig:
    """Reporting configuration."""
    output_dir: str = "reports"
    template_dir: str = "templates"
    dev_mode: bool = False
    
    def __post_init__(self):
        """Load values from environment variables."""
        self.dev_mode = os.getenv("DEV_MODE", "false").lower() == "true"

@dataclass
class StorageConfig:
    """Storage and caching configuration."""
    storage_dir: str = "storage"
    cache_dir: str = "storage/cache"
    logs_dir: str = "logs"
    
    def __post_init__(self):
        """Ensure directories exist."""
        Path(self.storage_dir).mkdir(exist_ok=True)
        Path(self.cache_dir).mkdir(exist_ok=True)
        Path(self.logs_dir).mkdir(exist_ok=True)

@dataclass
class Settings:
    """Main application settings."""
    environment: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    
    # Sub-configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    reporting: ReportingConfig = field(default_factory=ReportingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    
    def __post_init__(self):
        """Load environment-specific settings."""
        self.environment = os.getenv("ENVIRONMENT", self.environment)
        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)

    @classmethod
    def from_file(cls, config_path: str) -> "Settings":
        """Load settings from a JSON configuration file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        # Create instance with loaded data
        instance = cls()
        for key, value in config_data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        
        return instance
    
    def to_file(self, config_path: str):
        """Save current settings to a JSON file."""
        # Convert to dict, excluding complex objects
        config_dict = {
            "environment": self.environment,
            "debug": self.debug,
            "log_level": self.log_level,
        }
        
        with open(config_path, 'w') as f:
            json.dump(config_dict, f, indent=2)

# Global settings instance
settings = Settings()

# Environment-specific configurations
def get_config_for_environment(env: str) -> Dict[str, Any]:
    """Get configuration overrides for specific environments."""
    configs = {
        "development": {
            "debug": True,
            "log_level": "DEBUG",
        },
        "production": {
            "debug": False,
            "log_level": "INFO",
        },
        "testing": {
            "debug": True,
            "log_level": "DEBUG",
            "database": {
                "duckdb_path": ":memory:",
            }
        }
    }
    return configs.get(env, {})

def configure_for_environment(env: str):
    """Configure the global settings for a specific environment."""
    global settings
    env_config = get_config_for_environment(env)
    
    # Apply environment-specific overrides
    for key, value in env_config.items():
        if hasattr(settings, key):
            if isinstance(value, dict) and hasattr(getattr(settings, key), '__dict__'):
                # Handle nested configurations
                nested_obj = getattr(settings, key)
                for nested_key, nested_value in value.items():
                    setattr(nested_obj, nested_key, nested_value)
            else:
                setattr(settings, key, value)
