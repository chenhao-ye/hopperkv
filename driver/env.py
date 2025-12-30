# pointers to other built components
# should not depend on other source code files
from pathlib import Path

PROJ_PATH = Path(__file__).parent.parent.absolute()
REDIS_MODULE_PATH = PROJ_PATH / "hopperkv/redis_module/libhopper_redis_module.so"
CLIENT_MOD_PATH = "driver.client"
CLIENT_PRELOAD_MOD_PATH = "driver.client.preload"
