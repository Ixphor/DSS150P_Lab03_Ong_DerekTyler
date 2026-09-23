# Task 7.5

### Short explanation of how configuration is separated from code.

Configuration is separated from code by storing non-secret defaults in config/settings.yml and environment-specific secrets in a local .env file. The Python logic dynamically reads these files at runtime instead of hardcoding values. This ensures passwords are never committed to version control and allows the exact same pipeline code to run in different environments simply by swapping the configuration files.