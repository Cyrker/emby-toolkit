# routes/tmdb_collections.py

from flask import Blueprint, request, jsonify
import logging
from etk.compatibility.background import spawn, spawn_later
# 导入需要的模块
from database import custom_collection_db, tmdb_collection_db, settings_db
from extensions import admin_required, DELETING_COLLECTIONS
from handler import tmdb, tmdb_collections as collections_handler
import config_manager
import constants
from handler import emby

# 1. 创建电影合集蓝图
collections_bp = Blueprint('collections', __name__, url_prefix='/api/collections')

logger = logging.getLogger(__name__)
_MISSING_COLLECTION_CLEANUPS = set()


def _collection_image_url(path):
    if not path:
        return None
    path = str(path)
    if path.startswith(('http://', 'https://')):
        return path
    return f"https://image.tmdb.org/t/p/original/{path.lstrip('/')}"


def _collection_provider_payload(data):
    if not data:
        return None
    tmdb_id = str(data.get('tmdb_collection_id') or data.get('id') or '').strip()
    if not tmdb_id:
        return None
    backdrop = _collection_image_url(data.get('backdrop_path'))
    return {
        'item_type': 'BoxSet',
        'tmdb_id': tmdb_id,
        'name': data.get('name'),
        'overview': data.get('overview'),
        'actors_ready': False,
        'genres': [],
        'tags': [],
        'studios': [],
        'people': [],
        'images': {
            'primary': _collection_image_url(data.get('poster_path')),
            'backdrop': backdrop,
            'logo': None,
            'thumb': backdrop,
        },
    }


def _persist_collection_details(details, existing=None):
    parts = details.get('parts') or []
    tmdb_collection_db.upsert_native_collection({
        'tmdb_collection_id': details.get('id'),
        'emby_collection_id': (existing or {}).get('emby_collection_id'),
        'name': details.get('name') or (existing or {}).get('name'),
        'overview': details.get('overview'),
        'poster_path': details.get('poster_path'),
        'backdrop_path': details.get('backdrop_path'),
        'all_tmdb_ids': [str(item.get('id')) for item in parts if item.get('id')],
    })
    return tmdb_collection_db.get_native_collection_by_tmdb_id(details.get('id'))


def _find_matching_emby_collection_ids(tmdb_collection_id, row):
    app_config = config_manager.APP_CONFIG
    base_url = app_config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = app_config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    user_id = app_config.get(constants.CONFIG_OPTION_EMBY_USER_ID)
    if not all([base_url, api_key, user_id]):
        return [], base_url, api_key, user_id

    candidate_ids = []
    if (row or {}).get('emby_collection_id'):
        candidate_ids.append(str(row['emby_collection_id']))
    for collection in emby.get_all_native_collections_from_emby(base_url, api_key, user_id):
        if str(collection.get('tmdb_collection_id') or '') == tmdb_collection_id:
            candidate_ids.append(str(collection.get('emby_collection_id') or ''))

    verified_ids = []
    for collection_id in dict.fromkeys(value for value in candidate_ids if value):
        item = emby.get_emby_item_details(
            collection_id,
            base_url,
            api_key,
            user_id,
            fields='Type,ProviderIds,Name',
            silent_404=True,
        )
        provider_tmdb_id = str(((item or {}).get('ProviderIds') or {}).get('Tmdb') or '')
        if (item or {}).get('Type') == 'BoxSet' and provider_tmdb_id == tmdb_collection_id:
            verified_ids.append((collection_id, item))
    return verified_ids, base_url, api_key, user_id


def _cleanup_missing_collection(tmdb_collection_id, row):
    try:
        matches, base_url, api_key, user_id = _find_matching_emby_collection_ids(
            tmdb_collection_id,
            row,
        )
        if not all([base_url, api_key, user_id]):
            logger.warning(
                "  ➜ [合集清理] Emby 配置不完整，暂未删除 TMDb:%s 对应的 BoxSet。",
                tmdb_collection_id,
            )
        for collection_id, item in matches:
            DELETING_COLLECTIONS.add(collection_id)
            spawn_later(10, DELETING_COLLECTIONS.discard, collection_id)
            emby.empty_collection_in_emby(collection_id, base_url, api_key, user_id)
            remaining = emby.get_emby_item_details(
                collection_id,
                base_url,
                api_key,
                user_id,
                fields='Type',
                silent_404=True,
            )
            deleted = not remaining or emby.delete_item_with_token(
                collection_id,
                base_url,
                api_key,
                user_id,
            )
            if deleted:
                logger.info(
                    "  ➜ [合集清理] 已从 Emby 删除失效合集《%s》(TMDb:%s, Emby:%s)。",
                    item.get('Name') or (row or {}).get('name') or tmdb_collection_id,
                    tmdb_collection_id,
                    collection_id,
                )

        from database import image_policy_db
        from handler.media_image_cache import discard_unreferenced_policy_sources

        images = image_policy_db.delete_image_policy('BoxSet', tmdb_collection_id)
        deleted_row = tmdb_collection_db.delete_native_collection_by_tmdb_id(tmdb_collection_id)
        source_urls = [item.get('source_url') for item in images if isinstance(item, dict)]
        collection_row = deleted_row or row or {}
        source_urls.extend([
            _collection_image_url(collection_row.get('poster_path')),
            _collection_image_url(collection_row.get('backdrop_path')),
        ])
        discard_unreferenced_policy_sources(source_urls)
        logger.info(
            "  ➜ [合集清理] TMDb 合集 %s 已不存在，ETK 元数据和图片缓存已清理。",
            tmdb_collection_id,
        )
    finally:
        _MISSING_COLLECTION_CLEANUPS.discard(tmdb_collection_id)


def _schedule_missing_collection_cleanup(tmdb_collection_id, row):
    if tmdb_collection_id in _MISSING_COLLECTION_CLEANUPS:
        return
    _MISSING_COLLECTION_CLEANUPS.add(tmdb_collection_id)
    spawn(_cleanup_missing_collection, tmdb_collection_id, row)


@collections_bp.route('/provider/metadata/<tmdb_collection_id>', methods=['GET'])
def api_get_collection_provider_metadata(tmdb_collection_id):
    tmdb_collection_id = str(tmdb_collection_id or '').strip()
    if not tmdb_collection_id.isdigit():
        return jsonify({'error': 'invalid tmdb collection id'}), 400

    row = tmdb_collection_db.get_native_collection_by_tmdb_id(tmdb_collection_id)
    schema_version = int((row or {}).get('metadata_schema_version') or 0)
    if not row or schema_version < tmdb_collection_db.COLLECTION_METADATA_SCHEMA_VERSION:
        try:
            details = tmdb.get_collection_details(
                int(tmdb_collection_id),
                config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY),
                raise_not_found=True,
            )
        except tmdb.TmdbNotFoundError:
            logger.warning(
                "  ➜ [合集清理] TMDb 合集 %s 已返回 404，开始清理 ETK 与 Emby 中的失效合集。",
                tmdb_collection_id,
            )
            _schedule_missing_collection_cleanup(tmdb_collection_id, row)
            return jsonify({'error': 'tmdb collection no longer exists'}), 410
        if details:
            row = _persist_collection_details(details, existing=row)
    payload = _collection_provider_payload(row)
    if not payload:
        return jsonify({'error': 'collection metadata not found'}), 404
    from handler.media_image_cache import archive_metadata_images
    archive_metadata_images(payload, request.host_url, archive_sources=True)
    response = jsonify(payload)
    response.headers['Cache-Control'] = 'no-store'
    return response


@collections_bp.route('/provider/search', methods=['GET'])
def api_search_collection_provider():
    query = str(request.args.get('query') or '').strip()
    if not query:
        return jsonify([])

    query_key = query.casefold()
    merged = {}
    local_rows = tmdb_collection_db.get_all_native_collections()
    local_rows.sort(key=lambda row: (
        str(row.get('name') or '').casefold() != query_key,
        not str(row.get('name') or '').casefold().startswith(query_key),
        str(row.get('name') or '').casefold(),
    ))
    for row in local_rows:
        if query_key in str(row.get('name') or '').casefold():
            payload = _collection_provider_payload(row)
            if payload:
                merged[payload['tmdb_id']] = payload

    for item in tmdb.search_collections(
        query,
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY),
    ):
        payload = _collection_provider_payload(item)
        if payload and payload['tmdb_id'] not in merged:
            merged[payload['tmdb_id']] = payload
    return jsonify(list(merged.values())[:20])

# ======================================================================
# 读取操作 (Read Operations) - 负责动态组装数据
# ======================================================================

@collections_bp.route('/status', methods=['GET'])
@admin_required
def api_get_collections_status():
    """
    【V3 - 新架构核心】获取所有原生合集的完整状态。
    此端点现在会调用业务逻辑层来动态组装数据，而不是直接返回数据库内容。
    """
    try:
        # ★★★ 核心修正: 调用专门为前端组装数据的 handler 函数 ★★★
        # 这个函数只读取数据并进行处理，速度快，且返回前端需要的数据结构。
        final_results = collections_handler.assemble_all_collection_details()
        return jsonify(final_results)
    except Exception as e:
        logger.error(f"组装原生合集状态时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "读取合集时发生服务器内部错误"}), 500

# ======================================================================
# ★★★ 删除合集路由 ★★★
# ======================================================================
@collections_bp.route('/<emby_collection_id>', methods=['DELETE'])
@admin_required
def api_delete_collection(emby_collection_id):
    """
    删除指定的 Emby 合集。
    逻辑：先清空合集内的所有媒体项 -> 再删除合集条目本身。
    """
    logger.info(f"  ➜ 收到删除 Emby 合集请求 (ID: {emby_collection_id})")
    
    try:
        # 1. 获取配置
        app_config = config_manager.APP_CONFIG
        base_url = app_config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        api_key = app_config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        user_id = app_config.get(constants.CONFIG_OPTION_EMBY_USER_ID)

        if not all([base_url, api_key, user_id]):
            return jsonify({"error": "Emby 配置不完整，无法执行删除操作"}), 500
        DELETING_COLLECTIONS.add(emby_collection_id)
        def _clear_flag():
            DELETING_COLLECTIONS.discard(emby_collection_id)
        spawn_later(10, _clear_flag)
        # 2. 第一步：清空合集 (移除所有成员)
        # 这一步是为了防止 Emby 只是删除了合集壳子但没解绑关系，或者删除失败
        logger.info(f"  ➜ [删除合集] 步骤1: 正在清空合集 {emby_collection_id} 的成员...")
        empty_success = emby.empty_collection_in_emby(emby_collection_id, base_url, api_key, user_id)
        
        if not empty_success:
            logger.warning(f"  ➜ [删除合集] 清空合集成员失败，但将尝试强制删除合集条目。")

        # 3. 第二步：删除合集条目本身
        logger.info(f"  ➜ [删除合集] 步骤2: 正在删除合集条目 {emby_collection_id}...")
        delete_success = emby.delete_item(emby_collection_id, base_url, api_key, user_id)

        if delete_success:
            # 4. 取消激活，保留插件读取所需的合集元数据
            tmdb_collection_db.deactivate_native_collection_by_emby_id(emby_collection_id)
            return jsonify({"message": "合集已成功从 Emby 删除"}), 200
        else:
            return jsonify({"error": "删除合集失败，请检查 Emby 日志"}), 500

    except Exception as e:
        logger.error(f"删除合集时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": f"服务器内部错误: {str(e)}"}), 500
    
# ======================================================================
# ★★★ 新增：合集模块专属设置接口 ★★★
# ======================================================================
@collections_bp.route('/settings', methods=['GET'])
@admin_required
def api_get_collection_settings():
    """获取合集模块的设置 (结构化配置)"""
    try:
        # 1. 读取大配置对象，如果不存在则给默认值
        config = settings_db.get_setting('native_collections_config') or {}
        
        # 2. 确保返回给前端的数据包含默认字段 (防止前端报错)
        default_config = {
            "auto_sub_enabled": False,
            # 未来可以在这里加更多默认值，例如:
            # "exclude_genres": [],
            # "min_rating": 0
        }
        
        # 合并默认值 (数据库里的值覆盖默认值)
        final_config = {**default_config, **config}
        final_config.pop('auto_complete_enabled', None)
        
        return jsonify(final_config)
    except Exception as e:
        logger.error(f"获取合集设置失败: {e}")
        return jsonify({"error": "获取设置失败"}), 500

@collections_bp.route('/settings', methods=['POST'])
@admin_required
def api_save_collection_settings():
    """保存合集模块的设置 (支持增量更新)"""
    try:
        new_data = request.json
        if not isinstance(new_data, dict):
            return jsonify({"error": "无效的数据格式"}), 400

        new_data = {
            "auto_sub_enabled": bool(new_data["auto_sub_enabled"])
        } if "auto_sub_enabled" in new_data else {}

        # 1. 先读取旧配置
        current_config = settings_db.get_setting('native_collections_config') or {}
        current_config.pop('auto_complete_enabled', None)
        
        # 2. 更新字段 (增量更新，保留旧配置中未修改的字段)
        current_config.update(new_data)
        
        # 3. 保存回数据库
        settings_db.save_setting('native_collections_config', current_config)
        
        logger.info(f"API: 原生合集配置已更新: {current_config}")
            
        return jsonify({"message": "设置已保存"}), 200
    except Exception as e:
        logger.error(f"保存合集设置失败: {e}")
        return jsonify({"error": "保存设置失败"}), 500
