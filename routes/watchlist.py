# routes/watchlist.py

from flask import Blueprint, request, jsonify
import logging
from datetime import datetime, date
import config_manager
import constants
# 导入需要的模块

import task_manager
import extensions
from handler import emby, moviepilot
from extensions import admin_required, task_lock_required
from database import watchlist_db, settings_db


def _normalize_emby_ids(value):
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _get_watchlist_delete_context(tmdb_id):
    with watchlist_db.get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT title, emby_item_ids_json
                FROM media_metadata
                WHERE tmdb_id = %s AND item_type = 'Series'
                LIMIT 1
                """,
                (str(tmdb_id),),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "tmdb_id": str(tmdb_id),
        "title": row.get("title") or str(tmdb_id),
        "emby_ids": _normalize_emby_ids(row.get("emby_item_ids_json")),
    }


def _delete_watchlist_external_resources(tmdb_id):
    context = _get_watchlist_delete_context(tmdb_id)
    if not context:
        return {"success": False, "status": 404, "error": "未在追剧列表中找到该项目"}

    mp_config = settings_db.get_setting("mp_config") or {}
    mp_deleted = 0
    if mp_config.get("moviepilot_url"):
        for sub in moviepilot.find_subscriptions(str(tmdb_id), config=config_manager.APP_CONFIG):
            sub_id = sub.get("id")
            if not sub_id:
                continue
            if not moviepilot.delete_subscription_by_id(int(sub_id), config_manager.APP_CONFIG):
                return {"success": False, "status": 500, "error": "MoviePilot 取消订阅失败", "item": context}
            mp_deleted += 1

    app_config = config_manager.APP_CONFIG or {}
    emby_url = app_config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    emby_api_key = app_config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    emby_user_id = app_config.get(constants.CONFIG_OPTION_EMBY_USER_ID)
    failed_emby_ids = []
    for emby_id in context["emby_ids"]:
        if not emby.delete_item(emby_id, emby_url, emby_api_key, emby_user_id):
            failed_emby_ids.append(emby_id)

    if failed_emby_ids:
        return {
            "success": False,
            "status": 500,
            "error": "Emby 媒体项删除失败",
            "failed_emby_ids": failed_emby_ids,
            "item": context,
        }

    return {
        "success": True,
        "item": context,
        "mp_cancelled": bool(mp_config.get("moviepilot_url")),
        "mp_deleted": mp_deleted,
        "emby_deleted": len(context["emby_ids"]),
    }
# 1. 创建追剧列表蓝图
watchlist_bp = Blueprint('watchlist', __name__, url_prefix='/api/watchlist')

logger = logging.getLogger(__name__)

_INTRO_DETECTION_TRIGGER_MODES = {'off', 'import', 'favorite', 'playback'}


def _normalize_intro_detection_trigger_mode(value):
    mode = str(value or '').strip().lower()
    return mode if mode in _INTRO_DETECTION_TRIGGER_MODES else 'off'


def _normalize_intro_detection_library_ids(value):
    if not isinstance(value, list):
        return []
    result = []
    seen = set()
    for item in value:
        library_id = str(item or '').strip()
        if library_id and library_id not in seen:
            seen.add(library_id)
            result.append(library_id)
    return result


# 2. 使用蓝图定义路由
@watchlist_bp.route('', methods=['GET']) # 注意：这里的路径是空的，因为前缀已经定义
@admin_required
def api_get_watchlist():
    logger.debug("API (Blueprint): 收到获取追剧列表的请求。")
    try:
        items = watchlist_db.get_all_watchlist_items()

        for item in items:
            # 1. 直接重命名字段，将 psycopg2 解析好的对象传递给前端
            #    前端将收到一个名为 'next_episode_to_air' 的对象 (或 null)
            item['next_episode_to_air'] = item.get('next_episode_to_air_json')
            if 'next_episode_to_air_json' in item:
                del item['next_episode_to_air_json']

            # 2. 对缺失信息做同样处理
            #    前端将收到一个名为 'missing_info' 的对象 (或 null)
            item['missing_info'] = item.get('missing_info_json')
            if 'missing_info_json' in item:
                del item['missing_info_json']

            # 3. 格式化日期 (保留原有逻辑)
            for key, value in item.items():
                if isinstance(value, (datetime, date)):
                    item[key] = value.isoformat()
        
        return jsonify(items)
    except Exception as e:
        logger.error(f"获取追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取追剧列表时发生服务器内部错误"}), 500

@watchlist_bp.route('/add', methods=['POST'])
@admin_required
def api_add_to_watchlist():
    data = request.json
    item_id = data.get('item_id')
    tmdb_id = data.get('tmdb_id')
    item_name = data.get('item_name')
    item_type = data.get('item_type')

    if not all([item_id, tmdb_id, item_name, item_type]):
        return jsonify({"error": "缺少必要的项目信息"}), 400
    
    if item_type != 'Series':
        return jsonify({"error": "只能将'剧集'类型添加到追剧列表"}), 400

    logger.info(f"API (Blueprint): 收到手动添加 '{item_name}' 到追剧列表的请求。")
    
    try:
        watchlist_db.add_item_to_watchlist(
            tmdb_id=tmdb_id,
            item_name=item_name
        )
        return jsonify({"message": f"《{item_name}》已成功添加到追剧列表！"}), 200
    except Exception as e:
        logger.error(f"手动添加项目到追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在添加时发生内部错误"}), 500

@watchlist_bp.route('/update_status', methods=['POST'])
@admin_required
def api_update_watchlist_status():
    data = request.json
    item_id = data.get('item_id')
    new_status = data.get('new_status')

    if not item_id or new_status not in ['Watching', 'Ended', 'Paused', 'Pending', 'Completed']:
        return jsonify({"error": "请求参数无效"}), 400

    logger.info(f"  ➜ API (Blueprint): 收到请求，将项目 {item_id} 的追剧状态更新为 '{new_status}'。")
    try:
        success = watchlist_db.update_watchlist_item_status(
            tmdb_id=item_id,
            new_status=new_status
        )
        if success:
            return jsonify({"message": "状态更新成功"}), 200
        else:
            return jsonify({"error": "未在追剧列表中找到该项目"}), 404
    except Exception as e:
        logger.error(f"更新追剧状态时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在更新状态时发生内部错误"}), 500

@watchlist_bp.route('/remove/<item_id>', methods=['POST'])
@admin_required
def api_remove_from_watchlist(item_id):
    logger.info(f"  ➜ API (Blueprint): 收到请求，将项目 {item_id} 从追剧列表移除。")
    try:
        cleanup = _delete_watchlist_external_resources(item_id)
        if not cleanup.get("success"):
            return jsonify({
                "error": cleanup.get("error") or "删除外部资源失败",
                "details": cleanup,
            }), cleanup.get("status") or 500

        success = watchlist_db.remove_item_from_watchlist(
            tmdb_id=item_id 
        )
        if success:
            return jsonify({
                "message": "已从追剧列表移除，并已同步取消 MP 订阅、删除 Emby 媒体项",
                "cleanup": cleanup,
            }), 200
        else:
            return jsonify({"error": "未在追剧列表中找到该项目"}), 404
    except Exception as e:
        logger.error(f"从追剧列表移除项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "移除项目时发生未知的服务器内部错误"}), 500

@watchlist_bp.route('/refresh/<item_id>', methods=['POST'])
@admin_required
def api_trigger_single_watchlist_refresh(item_id):
    """触发单个剧集的刷新任务。"""
    from tasks.watchlist import task_process_watchlist 
    
    # 明确此时的 item_id 就是 tmdb_id
    tmdb_id = item_id
    
    logger.trace(f"API: 收到对单个追剧项目 (TMDb ID: {tmdb_id}) 的刷新请求。")
    if not extensions.watchlist_processor_instance:
        return jsonify({"error": "追剧处理模块未就绪"}), 503

    item_name = watchlist_db.get_watchlist_item_name(tmdb_id) or "未知剧集"

    task_manager.submit_task(
        task_process_watchlist,
        f"手动刷新: {item_name}",
        processor_type='watchlist',
        tmdb_id=tmdb_id,
    )
    
    return jsonify({"message": f"《{item_name}》的刷新任务已在后台启动！"}), 202

# --- 批量强制完结选中的季 ---
@watchlist_bp.route('/batch_force_end', methods=['POST'])
@admin_required
def api_batch_force_end_watchlist_items():
    """
    接收前端请求，批量强制完结选中的季。
    这可以解决因 TMDb 数据不准确导致已完结季被错误复活的问题，但不影响同剧其它季。
    """
    data = request.json
    item_ids = data.get('item_ids')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400

    logger.info(f"API (Blueprint): 收到对 {len(item_ids)} 个季的批量强制完结请求。")
    
    try:
        # 调用更新后的 watchlist_db 函数
        updated_count = watchlist_db.batch_force_end_watchlist_items(
            tmdb_ids=item_ids
        )
        
        return jsonify({
            "message": f"操作成功！已将 {updated_count} 个季标记为强制完结。它们不会因集数更新而复活，也不会影响同剧其它季。",
            "updated_count": updated_count
        }), 200
    except Exception as e:
        logger.error(f"批量强制完结项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量强制完结项目时发生未知的服务器内部错误"}), 500
    
# ★★★ 批量更新追剧状态的 API (用于“重新追剧”) ★★★
@watchlist_bp.route('/batch_update_status', methods=['POST'])
@admin_required
def api_batch_update_watchlist_status():
    """
    接收前端请求，批量更新选中项目的追剧状态。
    主要用于“已完结”列表中的“重新追剧”功能。
    """
    data = request.json
    item_ids = data.get('item_ids')
    new_status = data.get('new_status')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400
    
    # 增加对 new_status 的校验，确保只接受合法的状态
    if new_status not in ['Watching', 'Paused', 'Completed']:
        return jsonify({"error": f"无效的状态值: {new_status}"}), 400

    logger.info(f"API: 收到对 {len(item_ids)} 个项目的批量状态更新请求，新状态为 '{new_status}'。")
    
    try:
        # 调用 watchlist_db 中我们将要创建的新函数
        updated_count = watchlist_db.batch_update_watchlist_status(
            item_ids=item_ids,
            new_status=new_status
        )
        
        return jsonify({
            "message": f"操作成功！已将 {updated_count} 个项目的状态更新为 '{new_status}'。",
            "updated_count": updated_count
        }), 200
    except Exception as e:
        logger.error(f"批量更新项目状态时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量更新项目状态时发生未知的服务器内部错误"}), 500
    
@watchlist_bp.route('/batch_remove', methods=['POST'])
@admin_required
def api_batch_remove_from_watchlist():
    """
    接收前端请求，批量从追剧列表中移除项目。
    """
    data = request.json
    item_ids = data.get('item_ids')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400

    logger.info(f"API: 收到对 {len(item_ids)} 个项目的批量移除请求。")
    
    try:
        cleaned_ids = []
        failures = []
        cleanups = []
        for item_id in item_ids:
            cleanup = _delete_watchlist_external_resources(item_id)
            if cleanup.get("success"):
                cleaned_ids.append(item_id)
                cleanups.append(cleanup)
            else:
                failures.append({
                    "tmdb_id": item_id,
                    "error": cleanup.get("error") or "删除外部资源失败",
                    "details": cleanup,
                })

        removed_count = watchlist_db.batch_remove_from_watchlist(cleaned_ids) if cleaned_ids else 0
        status_code = 207 if failures and cleaned_ids else (500 if failures else 200)
        
        return jsonify({
            "message": f"已移除 {removed_count} 个追剧项目；同步删除失败 {len(failures)} 个。",
            "removed_count": removed_count,
            "cleanup": cleanups,
            "failures": failures,
        }), status_code
    except Exception as e:
        logger.error(f"批量移除项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量移除项目时发生未知的服务器内部错误"}), 500
    
@watchlist_bp.route('/update_total_episodes', methods=['POST'])
@admin_required
def api_update_total_episodes():
    data = request.json
    tmdb_id = data.get('tmdb_id')
    total_episodes = data.get('total_episodes')
    
    # ★★★ 新增：前端传 item_type，默认为 Series 以兼容旧代码，但现在我们主要用 Season ★★★
    item_type = data.get('item_type', 'Season') 

    if not tmdb_id or total_episodes is None:
        return jsonify({"error": "参数无效"}), 400

    try:
        with watchlist_db.get_db_connection() as conn:
            cursor = conn.cursor()
            # 更新集数 并 开启锁定
            sql = """
                UPDATE media_metadata
                SET total_episodes = %s, total_episodes_locked = TRUE
                WHERE tmdb_id = %s AND item_type = %s
            """
            cursor.execute(sql, (total_episodes, tmdb_id, item_type))
            conn.commit()
            return jsonify({"message": f"已修正总集数为 {total_episodes} 并锁定。"}), 200
    except Exception as e:
        logger.error(f"手动更新总集数失败: {e}", exc_info=True)
        return jsonify({"error": "数据库更新失败"}), 500
    
@watchlist_bp.route('/settings', methods=['GET', 'POST'])
@admin_required
def api_watchlist_settings():
    """
    GET: 获取追剧策略配置
    POST: 保存追剧策略配置
    配置直接存储在 app_settings 表的 'watchlist_config' 键中
    """
    CONFIG_KEY = 'watchlist_config'
    
    # 默认配置结构
    default_config = {
        "douban_count_correction": False,
        "revival_check_days": 365,
        "tg_channel_tracking": False,
        "intro_detection_trigger_mode": "off",
        "intro_detection_credits_enabled": False,
        "intro_detection_library_ids": [],
    }

    if request.method == 'GET':
        try:
            # 直接从数据库读取
            saved_config = settings_db.get_setting(CONFIG_KEY) or {}
            # 合并默认值，确保字段完整
            saved_config = saved_config if isinstance(saved_config, dict) else {}
            final_config = {
                **default_config,
                **{k: v for k, v in saved_config.items() if k in default_config},
            }
            if 'intro_detection_trigger_mode' in saved_config:
                final_config['intro_detection_trigger_mode'] = _normalize_intro_detection_trigger_mode(
                    saved_config.get('intro_detection_trigger_mode')
                )
            elif settings_db.migrate_legacy_intro_detection_trigger_mode():
                final_config['intro_detection_trigger_mode'] = 'import'
            final_config['intro_detection_library_ids'] = _normalize_intro_detection_library_ids(
                final_config.get('intro_detection_library_ids')
            )
            return jsonify(final_config), 200
        except Exception as e:
            logger.error(f"获取追剧配置失败: {e}", exc_info=True)
            return jsonify({"error": "获取配置失败"}), 500

    elif request.method == 'POST':
        try:
            new_config = request.json
            if not isinstance(new_config, dict):
                return jsonify({"error": "配置格式错误"}), 400
            current_config = settings_db.get_setting(CONFIG_KEY) or {}
            current_config = current_config if isinstance(current_config, dict) else {}
            if (
                'intro_detection_trigger_mode' not in current_config
                and 'intro_detection_trigger_mode' not in new_config
                and settings_db.migrate_legacy_intro_detection_trigger_mode()
            ):
                current_config = {
                    **current_config,
                    'intro_detection_trigger_mode': 'import',
                }

            updated_config = dict(current_config)
            for key, default in default_config.items():
                if key in new_config:
                    updated_config[key] = new_config[key]
                elif key not in updated_config:
                    updated_config[key] = default
            updated_config['intro_detection_trigger_mode'] = _normalize_intro_detection_trigger_mode(
                updated_config.get('intro_detection_trigger_mode')
            )
            updated_config['intro_detection_library_ids'] = _normalize_intro_detection_library_ids(
                updated_config.get('intro_detection_library_ids')
            )
            
            # 保存到数据库
            settings_db.save_setting(CONFIG_KEY, updated_config)
            return jsonify({"message": "追剧策略配置已保存"}), 200
        except Exception as e:
            logger.error(f"保存追剧配置失败: {e}", exc_info=True)
            return jsonify({"error": "保存配置失败"}), 500
