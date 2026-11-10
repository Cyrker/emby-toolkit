"""独立发布来源，以及接续现有 Emby 安装所需的插件兼容标识。"""

PLUGIN_NAME = "embytool Bridge"
PLUGIN_NAMES = (PLUGIN_NAME, "ETK MediaInfo Bridge")
PLUGIN_DLL_NAME = "ETKMediaInfoBridge.dll"
PLUGIN_TASK_KEY = "ETKMediaInfoBridgeUpdate"
PLUGIN_RELEASE_API = "https://api.github.com/repos/Cyrker/embytool-bridge/releases/latest"
PLUGIN_DOWNLOAD_URL = f"https://github.com/Cyrker/embytool-bridge/releases/latest/download/{PLUGIN_DLL_NAME}"
