import os

env = os.getenv("ENV", "deva")

if env == "deva":
    from config.dev_config import config_dev
else:
    from config.prod_config import config_prod