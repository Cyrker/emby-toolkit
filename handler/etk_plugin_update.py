import io
import logging
import re
import tarfile
import threading
import time

import requests

import config_manager
import constants
import docker
from etk.compatibility.background import spawn_later
from etk.domains.media.plugin_identity import (
    PLUGIN_DLL_NAME,
    PLUGIN_NAME,
    PLUGIN_NAMES,
    PLUGIN_TASK_KEY,
)
from etk.domains.media.plugin_identity import (
    PLUGIN_DOWNLOAD_URL as LATEST_RELEASE_DOWNLOAD,
)
from etk.domains.media.plugin_identity import (
    PLUGIN_RELEASE_API as LATEST_RELEASE_API,
)
from handler.emby import configure_etk_plugin_origin, emby_client

logger = logging.getLogger(__name__)

PLUGIN_INSTALL_ALLOW_LABEL = 'com.embytoolkit.control-plane.plugin-install'
_START_LOCK = threading.Lock()
_STARTED = False


def _version_tuple(value):
    numbers = [int(part) for part in re.findall(r'\d+', str(value or ''))[:4]]
    return tuple((numbers + [0, 0, 0, 0])[:4]) if numbers else None


def _emby_headers(api_key):
    return {'X-Emby-Token': api_key, 'Accept': 'application/json'}


def _emby_get_json(base_url, api_key, path):
    response = emby_client.get(
        f"{base_url.rstrip('/')}{path}",
        headers=_emby_headers(api_key),
        params={'api_key': api_key},
    )
    response.raise_for_status()
    return response.json()


def _latest_plugin_version():
    headers = {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'embytool-PluginUpdater',
    }
    token = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN) or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'
    response = requests.get(
        LATEST_RELEASE_API,
        headers=headers,
        timeout=20,
        proxies=config_manager.get_proxies_for_requests(),
    )
    if response.status_code == 404:
        return '', None
    response.raise_for_status()
    tag = str((response.json() or {}).get('tag_name') or '').strip()
    return tag, _version_tuple(tag)


def _find_plugin(plugins):
    for plugin in plugins or []:
        if str(plugin.get('Name') or '').strip() in PLUGIN_NAMES:
            return plugin
    return None


def is_etk_plugin_installed(base_url=None, api_key=None):
    base_url = str(base_url or config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').strip()
    api_key = str(api_key or config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    if not base_url or not api_key:
        return False
    try:
        return _find_plugin(_emby_get_json(base_url, api_key, '/Plugins')) is not None
    except Exception:
        return False


def _plugin_download_headers():
    headers = {'User-Agent': 'embytool-PluginInstaller'}
    token = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN) or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _download_latest_plugin_dll():
    response = requests.get(
        LATEST_RELEASE_DOWNLOAD,
        headers=_plugin_download_headers(),
        timeout=(15, 300),
        proxies=config_manager.get_proxies_for_requests(),
    )
    response.raise_for_status()
    content = response.content
    if len(content) < 10_000 or not content.startswith(b'MZ'):
        raise RuntimeError('下载结果不是有效的 ETK 插件 DLL')
    return content


def _container_network_values(container):
    attrs = container.attrs or {}
    networks = ((attrs.get('NetworkSettings') or {}).get('Networks') or {})
    ips = set()
    aliases = {str(container.name or '').strip().lower()}
    for network in networks.values():
        ip = str((network or {}).get('IPAddress') or '').strip()
        if ip:
            ips.add(ip)
        for alias in (network or {}).get('Aliases') or []:
            if alias:
                aliases.add(str(alias).strip().lower())
    return ips, aliases


def _is_emby_container(container):
    attrs = container.attrs or {}
    config = attrs.get('Config') or {}
    image = str((config.get('Image')) or '').lower()
    name = str(container.name or '').lower()
    labels = config.get('Labels') or {}
    service = str(labels.get('com.docker.compose.service') or '').lower()

    # ETK's own container/image contains "emby-toolkit"; do not count it as an
    # Emby server candidate, especially in host-network deployments.
    # Compose metadata also contains host paths such as project.config_files.
    # Those paths may include "emby-toolkit" for every service in the project,
    # so only container identity fields participate in the ETK self-exclusion.
    haystack = ' '.join([image, name, service])
    excluded_markers = (
        'embytool',
        'emby-toolkit',
        'etk-mediainfo-bridge',
        'hbq0405/emby-toolkit',
    )
    if any(marker in haystack for marker in excluded_markers):
        return False

    image_repo = image.split(':', 1)[0]
    name_tokens = {token for token in re.split(r'[^a-z0-9]+', name) if token}
    image_tokens = {token for token in re.split(r'[^a-z0-9]+', image_repo) if token}
    return (
        image_repo == 'emby/embyserver'
        or image_repo.endswith('/embyserver')
        or image_repo.endswith('/emby')
        or 'embyserver' in image_tokens
        or 'embyserver' in name_tokens
        or name_tokens == {'emby'}
    )


def _candidate_emby_urls(container):
    attrs = container.attrs or {}
    exposed = ((attrs.get('Config') or {}).get('ExposedPorts') or {})
    ports = {8096}
    for value in exposed:
        text = str(value).split('/', 1)[0]
        if text.isdigit():
            ports.add(int(text))
    ips, _aliases = _container_network_values(container)
    return [f'http://{ip}:{port}' for ip in ips for port in sorted(ports)]


def _container_label(container):
    attrs = container.attrs or {}
    image = str(((attrs.get('Config') or {}).get('Image')) or '').strip()
    return f"{container.name or '<unknown>'} ({image or 'unknown image'})"


def _is_plugin_install_allowlisted(container, allowed_services):
    attrs = container.attrs or {}
    labels = ((attrs.get('Config') or {}).get('Labels') or {})
    service = str(labels.get('com.docker.compose.service') or '').strip()
    opted_in = str(labels.get(PLUGIN_INSTALL_ALLOW_LABEL) or '').strip().lower() == 'true'
    return opted_in or service in allowed_services


def _public_server_id(base_url):
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(f"{base_url.rstrip('/')}/System/Info/Public", timeout=3)
        response.raise_for_status()
        data = response.json() or {}
        return str(data.get('Id') or data.get('ServerId') or '').strip()
    except Exception:
        return ''
    finally:
        session.close()


def _resolve_emby_container(client, base_url, api_key, *, allowed_services=()):
    target_info = _emby_get_json(base_url, api_key, '/System/Info')
    target_server_id = str(target_info.get('Id') or target_info.get('ServerId') or '').strip()
    if not target_server_id:
        raise RuntimeError('已授权的 Emby 未返回稳定 Server ID，拒绝修改任何 Docker 容器')

    service_allowlist = {str(value).strip() for value in allowed_services if str(value).strip()}
    containers = [
        item
        for item in client.containers.list()
        if _is_emby_container(item) and _is_plugin_install_allowlisted(item, service_allowlist)
    ]
    matched = []
    for container in containers:
        if any(
            _public_server_id(url) == target_server_id
            for url in _candidate_emby_urls(container)
        ):
            matched.append(container)
    if len(matched) == 1:
        return matched[0]
    labels = ', '.join(_container_label(item) for item in containers) or 'none'
    raise RuntimeError(
        '无法在插件安装 allowlist 中唯一定位 Server ID 与已授权服务严格一致的 Emby 容器，'
        f'候选容器: {labels}'
    )


def _resolve_plugins_path(container):
    attrs = container.attrs or {}
    mounts = attrs.get('Mounts') or []
    candidates = []
    for mount in mounts:
        destination = str((mount or {}).get('Destination') or '').rstrip('/')
        if destination == '/config':
            candidates.append('/config/plugins')
        elif destination.endswith('/emby'):
            candidates.append(destination + '/plugins')
    candidates.extend(['/config/plugins', '/var/lib/emby/plugins'])
    for path in dict.fromkeys(candidates):
        result = container.exec_run(['test', '-d', path])
        if int(result.exit_code or 0) == 0:
            return path
    raise RuntimeError('无法确定 Emby 插件目录')


def _put_plugin_dll(container, plugins_path, content):
    target_path = f"{plugins_path.rstrip('/')}/{PLUGIN_DLL_NAME}"
    if int(container.exec_run(['test', '-f', target_path]).exit_code or 0) == 0:
        container.exec_run(['cp', '-f', target_path, target_path + '.bak'])
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w') as tar:
        info = tarfile.TarInfo(PLUGIN_DLL_NAME)
        info.size = len(content)
        info.mode = 0o644
        info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(content))
    archive.seek(0)
    if not container.put_archive(plugins_path, archive.read()):
        raise RuntimeError('写入 Emby 插件目录失败')


def _wait_for_plugin(base_url, api_key, timeout_seconds=180):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            plugin = _find_plugin(_emby_get_json(base_url, api_key, '/Plugins'))
            if plugin is not None:
                return plugin
        except Exception:
            pass
        time.sleep(2)
    return None


def preflight_etk_plugin_install(base_url, api_key, *, allowed_services=()):
    """Validate authorization and the local Docker target without writing it."""
    base_url = str(base_url or '').strip().rstrip('/')
    api_key = str(api_key or '').strip()
    if not base_url or not api_key:
        raise ValueError('缺少 Emby 服务地址或授权 Token')

    plugins = _emby_get_json(base_url, api_key, '/Plugins')
    if _find_plugin(plugins) is not None:
        return {'already_installed': True}

    client = docker.from_env()
    try:
        container = _resolve_emby_container(
            client,
            base_url,
            api_key,
            allowed_services=allowed_services,
        )
        plugins_path = _resolve_plugins_path(container)
        return {
            'already_installed': False,
            'container_name': str(container.name or ''),
            'plugins_path': plugins_path,
        }
    finally:
        client.close()


def ensure_etk_plugin_installed(base_url, api_key, etk_url='', *, allowed_services=(), effect_started=None):
    """Install the bridge into the authorized Docker Emby, then restart and verify it."""
    base_url = str(base_url or '').strip().rstrip('/')
    api_key = str(api_key or '').strip()
    etk_url = str(etk_url or '').strip().rstrip('/')
    if not base_url or not api_key:
        return {'ok': False, 'status': 'failed', 'message': '缺少 Emby 服务地址或授权 Token'}

    try:
        if is_etk_plugin_installed(base_url, api_key):
            if etk_url and callable(effect_started):
                effect_started()
            origin_configured = bool(etk_url and configure_etk_plugin_origin(base_url, api_key, etk_url))
            return {
                'ok': True,
                'status': 'already_installed',
                'origin_configured': origin_configured,
                'message': f'{PLUGIN_NAME} 已安装',
            }

        logger.info('  ➜ [插件自动安装] Emby 尚未安装 %s，开始下载安装。', PLUGIN_NAME)
        client = docker.from_env()
        try:
            container = _resolve_emby_container(
                client,
                base_url,
                api_key,
                allowed_services=allowed_services,
            )
            plugins_path = _resolve_plugins_path(container)
            content = _download_latest_plugin_dll()
            if callable(effect_started):
                effect_started()
            _put_plugin_dll(container, plugins_path, content)
            logger.info('  ➜ [插件自动安装] 插件已写入 Emby 容器，正在重启使其生效。')
            container.restart(timeout=10)
        finally:
            client.close()

        plugin = _wait_for_plugin(base_url, api_key)
        if plugin is None:
            raise RuntimeError(f'Emby 重启后未检测到 {PLUGIN_NAME}')
        origin_configured = bool(etk_url and configure_etk_plugin_origin(base_url, api_key, etk_url))
        version = str(plugin.get('Version') or '').strip()
        logger.info('  ➜ [插件自动安装] %s %s 安装完成。', PLUGIN_NAME, version or '')
        return {
            'ok': True,
            'status': 'installed',
            'version': version,
            'origin_configured': origin_configured,
            'message': f"{PLUGIN_NAME} {version or ''} 已自动安装并重启 Emby".strip(),
        }
    except Exception as e:
        logger.error('  ➜ [插件自动安装] 自动安装失败: %s', e, exc_info=True)
        return {'ok': False, 'status': 'failed', 'message': str(e)}


def _find_update_task(tasks):
    for task in tasks or []:
        if str(task.get('Key') or '').strip() == PLUGIN_TASK_KEY:
            return task
    return None


def _task_end_time(task):
    return str(((task or {}).get('LastExecutionResult') or {}).get('EndTimeUtc') or '').strip()


def _wait_for_task(base_url, api_key, task_id, previous_end_time, timeout_seconds=420):
    deadline = time.monotonic() + timeout_seconds
    saw_running = False
    while time.monotonic() < deadline:
        tasks = _emby_get_json(base_url, api_key, '/ScheduledTasks')
        task = next((item for item in tasks if str(item.get('Id')) == str(task_id)), None)
        if task is None:
            raise RuntimeError('Emby 插件更新任务已消失')
        if str(task.get('State') or '').lower() == 'running':
            saw_running = True
            time.sleep(2)
            continue

        result = task.get('LastExecutionResult') or {}
        end_time = str(result.get('EndTimeUtc') or '').strip()
        if saw_running or (end_time and end_time != previous_end_time):
            status = str(result.get('Status') or '').strip()
            if status.lower() == 'completed':
                return True
            logger.error(
                "  ➜ [插件联动更新] Emby 更新任务失败: status=%s, error=%s",
                status or 'unknown',
                result.get('ErrorMessage') or result.get('LongErrorMessage') or '',
            )
            return False
        time.sleep(2)
    logger.error("  ➜ [插件联动更新] 等待 Emby 插件更新任务超时。")
    return False


def check_and_update_etk_plugin():
    base_url = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').strip()
    api_key = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    if not base_url or not api_key:
        logger.debug("  ➜ [插件联动更新] 未配置 Emby 地址或 API Key，跳过检查。")
        return False

    try:
        plugin = _find_plugin(_emby_get_json(base_url, api_key, '/Plugins'))
        if plugin is None:
            logger.warning("  ➜ [插件联动更新] Emby 未安装 %s，跳过自动更新。", PLUGIN_NAME)
            return False
        current_text = str(plugin.get('Version') or '').strip()
        current_version = _version_tuple(current_text)
        latest_text, latest_version = _latest_plugin_version()
        if current_version is None or latest_version is None:
            logger.warning(
                "  ➜ [插件联动更新] 无法解析插件版本: current=%s, latest=%s",
                current_text or 'unknown',
                latest_text or 'unknown',
            )
            return False
        if current_version >= latest_version:
            logger.debug(
                "  ➜ [插件联动更新] 插件已是最新版: %s",
                current_text,
            )
            return False

        tasks = _emby_get_json(base_url, api_key, '/ScheduledTasks')
        task = _find_update_task(tasks)
        if task is None:
            logger.error("  ➜ [插件联动更新] Emby 中未找到插件更新计划任务。")
            return False

        task_id = str(task.get('Id') or '').strip()
        if not task_id:
            logger.error("  ➜ [插件联动更新] Emby 插件更新任务缺少 ID。")
            return False
        previous_end_time = _task_end_time(task)
        logger.info(
            "  ➜ [插件联动更新] 检测到新版本: %s -> %s，正在触发 Emby 更新任务。",
            current_text,
            latest_text,
        )
        if str(task.get('State') or '').lower() != 'running':
            response = emby_client.post(
                f"{base_url.rstrip('/')}/ScheduledTasks/Running/{task_id}",
                headers=_emby_headers(api_key),
                params={'api_key': api_key},
            )
            response.raise_for_status()

        if not _wait_for_task(base_url, api_key, task_id, previous_end_time):
            return False

        system_info = _emby_get_json(base_url, api_key, '/System/Info')
        if not bool(system_info.get('HasPendingRestart')):
            logger.error("  ➜ [插件联动更新] 更新任务已完成，但 Emby 未报告待重启，拒绝自动重启。")
            return False

        logger.info("  ➜ [插件联动更新] 插件已更新，正在自动重启 Emby 使其生效。")
        response = emby_client.post(
            f"{base_url.rstrip('/')}/System/Restart",
            headers=_emby_headers(api_key),
            params={'api_key': api_key},
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error("  ➜ [插件联动更新] 自动检查或更新失败: %s", e)
        return False


def schedule_etk_plugin_update_check(delay_seconds=30):
    global _STARTED
    with _START_LOCK:
        if _STARTED:
            return False
        _STARTED = True

    spawn_later(delay_seconds, check_and_update_etk_plugin)
    return True
