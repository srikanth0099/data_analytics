import os

env = os.getenv("ENV", "dev")

if env == "dev":
    from config.dev_config import config_dev
else:
    from config.prod_config import config_prod