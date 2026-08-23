# database/settings_db.py
import psycopg2
import logging
import json
import re
import threading
import pytz
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from typing import Optional, Any, Dict
from datetime import datetime

from .connection import get_db_connection
from etk.platform.config.locking import (
    DYNAMIC_CONFIG_LOCK_KEY,
    DYNAMIC_CONFIG_LOCK_NAMESPACE,
    P115_AUTH_LOCK_KEY,
    P115_UNRECOGNIZED_DELETE_LOCK_KEY,
)
import config_manager
import constants

logger = logging.getLogger(__name__)
WASHING_PRIORITY_RECALCULATE_SETTING_KEY = 'p115_washing_priority_recalculate_state'
_dynamic_config_lock_state = threading.local()
_p115_auth_lock_state = threading.local()
_p115_unrecognized_delete_lock_state = threading.local()

# ======================================================================
# 模块: 配置数据访问
# ======================================================================

def get_setting(setting_key: str) -> Optional[Any]:
    """从 app_settings 表中获取一个设置项的值。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (setting_key,))
            row = cursor.fetchone()
            return row['value_json'] if row else None
    except Exception as e:
        logger.error(f"DB: 获取设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise

def _save_setting_with_cursor(cursor, setting_key: str, value: Dict[str, Any]):
    """【内部函数】使用一个已有的数据库游标来保存设置。"""
    
    sql = """
        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (setting_key) DO UPDATE SET
            value_json = EXCLUDED.value_json,
            last_updated_at = NOW();
    """
    value_as_json = json.dumps(value, ensure_ascii=False)
    cursor.execute(sql, (setting_key, value_as_json))

def save_setting(setting_key: str, value: Dict[str, Any], *, log_success: bool = True):
    """【V2 - 重构版】向 app_settings 表中保存或更新一个设置项。"""

    if setting_key == 'dynamic_app_config':
        with dynamic_config_write_lock():
            return _save_setting_unlocked(setting_key, value, log_success=log_success)
    return _save_setting_unlocked(setting_key, value, log_success=log_success)


def _save_setting_unlocked(setting_key: str, value: Dict[str, Any], *, log_success: bool = True):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            _save_setting_with_cursor(cursor, setting_key, value)
            conn.commit()
            if log_success:
                logger.trace(f"  ➜ 成功保存设置 '{setting_key}'。")
    except Exception as e:
        logger.error(f"  ➜ 保存设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise


@contextmanager
def dynamic_config_write_lock():
    """Serialize dynamic config mutation and runtime compensation across processes."""

    depth = int(getattr(_dynamic_config_lock_state, 'depth', 0))
    if depth:
        _dynamic_config_lock_state.depth = depth + 1
        try:
            yield
        finally:
            _dynamic_config_lock_state.depth = depth
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pg_advisory_lock(%s, %s)",
            (DYNAMIC_CONFIG_LOCK_NAMESPACE, DYNAMIC_CONFIG_LOCK_KEY),
        )
        conn.commit()
        _dynamic_config_lock_state.depth = 1
        try:
            yield
        finally:
            _dynamic_config_lock_state.depth = 0
            cursor.execute(
                "SELECT pg_advisory_unlock(%s, %s)",
                (DYNAMIC_CONFIG_LOCK_NAMESPACE, DYNAMIC_CONFIG_LOCK_KEY),
            )
            conn.commit()


@contextmanager
def p115_auth_write_lock():
    """Serialize 115 credential replacement and Quick Deploy account snapshots."""

    depth = int(getattr(_p115_auth_lock_state, 'depth', 0))
    if depth:
        _p115_auth_lock_state.depth = depth + 1
        try:
            yield
        finally:
            _p115_auth_lock_state.depth = depth
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pg_advisory_lock(%s, %s)",
            (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY),
        )
        conn.commit()
        _p115_auth_lock_state.depth = 1
        try:
            yield
        finally:
            _p115_auth_lock_state.depth = 0
            cursor.execute(
                "SELECT pg_advisory_unlock(%s, %s)",
                (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY),
            )
            conn.commit()


@contextmanager
def p115_unrecognized_delete_lock():
    """Serialize destructive cleanup attempts for the configured unrecognized directory."""

    depth = int(getattr(_p115_unrecognized_delete_lock_state, 'depth', 0))
    if depth:
        _p115_unrecognized_delete_lock_state.depth = depth + 1
        try:
            yield
        finally:
            _p115_unrecognized_delete_lock_state.depth = depth
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pg_advisory_lock(%s, %s)",
            (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_UNRECOGNIZED_DELETE_LOCK_KEY),
        )
        conn.commit()
        _p115_unrecognized_delete_lock_state.depth = 1
        try:
            yield
        finally:
            _p115_unrecognized_delete_lock_state.depth = 0
            cursor.execute(
                "SELECT pg_advisory_unlock(%s, %s)",
                (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_UNRECOGNIZED_DELETE_LOCK_KEY),
            )
            conn.commit()


def merge_setting_mapping(setting_key: str, updates: Mapping[str, Any]) -> Dict[str, Any]:
    """Atomically merge object fields and return the committed mapping."""

    with dynamic_config_write_lock():
        return _merge_setting_mapping_unlocked(setting_key, updates)


def _merge_setting_mapping_unlocked(setting_key: str, updates: Mapping[str, Any]) -> Dict[str, Any]:
    sql = """
        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (setting_key) DO UPDATE SET
            value_json = COALESCE(app_settings.value_json, '{}'::jsonb) || EXCLUDED.value_json,
            last_updated_at = NOW()
        RETURNING value_json;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (setting_key, json.dumps(dict(updates), ensure_ascii=False)))
            row = cursor.fetchone()
            conn.commit()
    except Exception as e:
        logger.error(f"  ➜ 合并设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise

    if row is None:
        raise RuntimeError(f"合并设置 '{setting_key}' 时数据库未返回结果")
    value = row['value_json'] if isinstance(row, Mapping) else row[0]
    if not isinstance(value, dict):
        raise TypeError(f"设置 '{setting_key}' 必须是 JSON 对象")
    return dict(value)


def update_setting_mapping(
    setting_key: str,
    updater: Callable[[Dict[str, Any]], Mapping[str, Any]],
) -> Dict[str, Any]:
    """Update one object setting while holding its PostgreSQL row lock."""

    with dynamic_config_write_lock():
        return _update_setting_mapping_unlocked(setting_key, updater)


def _update_setting_mapping_unlocked(
    setting_key: str,
    updater: Callable[[Dict[str, Any]], Mapping[str, Any]],
) -> Dict[str, Any]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES (%s, '{}'::jsonb, NOW())
                ON CONFLICT (setting_key) DO NOTHING
                """,
                (setting_key,),
            )
            cursor.execute(
                "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                (setting_key,),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"更新设置 '{setting_key}' 时数据库未返回结果")
            value = row['value_json'] if isinstance(row, Mapping) else row[0]
            if not isinstance(value, dict):
                raise TypeError(f"设置 '{setting_key}' 必须是 JSON 对象")
            updated = updater(dict(value))
            if not isinstance(updated, Mapping):
                raise TypeError("设置更新函数必须返回映射")
            committed = dict(updated)
            cursor.execute(
                """
                UPDATE app_settings
                SET value_json = %s, last_updated_at = NOW()
                WHERE setting_key = %s
                """,
                (json.dumps(committed, ensure_ascii=False), setting_key),
            )
            conn.commit()
            return committed
    except Exception as e:
        logger.error(f"  ➜ 更新设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise


def restore_setting_keys_if_unchanged(
    setting_key: str,
    expected_updates: Mapping[str, Any],
    previous_values: Mapping[str, Any],
) -> Dict[str, Any]:
    """Restore only keys still equal to this writer's values."""

    def restore(current: Dict[str, Any]) -> Dict[str, Any]:
        for key, expected in expected_updates.items():
            if key not in current or current[key] != expected:
                continue
            if key in previous_values:
                current[key] = previous_values[key]
            else:
                current.pop(key, None)
        return current

    return update_setting_mapping(setting_key, restore)

def _normalize_washing_conflict_mode(value: Any, default: str = 'replace') -> str:
    mode = str(value or default or '').strip().lower()
    aliases = {
        'overwrite': 'replace',
        'washing': 'replace',
        'wash': 'replace',
        '洗版': 'replace',
        '替换': 'replace',
        'skip_existing': 'skip',
        '跳过': 'skip',
        'keep': 'keep_both',
        'both': 'keep_both',
        'keepboth': 'keep_both',
        'keep-both': 'keep_both',
        '保留两者': 'keep_both',
        '共存': 'keep_both',
    }
    mode = aliases.get(mode, mode)
    return mode if mode in {'replace', 'keep_both', 'skip'} else default

def _normalize_washing_skip_scope(value: Any, default: str = 'directory') -> str:
    scope = str(value or default or '').strip().lower()
    aliases = {
        'dir': 'directory',
        'same_dir': 'directory',
        'same-directory': 'directory',
        'same_directory': 'directory',
        'folder': 'directory',
        'current': 'directory',
        '同目录': 'directory',
        '当前目录': 'directory',
        'all': 'library',
        'global': 'library',
        'full': 'library',
        'full_library': 'library',
        'full-library': 'library',
        'all_library': 'library',
        'all-library': 'library',
        'library': 'library',
        '全库': 'library',
        '媒体库': 'library',
    }
    scope = aliases.get(scope, scope)
    return scope if scope in {'directory', 'library'} else default


def _normalize_washing_version_slots(value: Any) -> list:
    slots = _washing_json_list(value)
    normalized = []
    list_fields = (
        'resolution',
        'source',
        'codec',
        'effect',
        'audio',
        'subtitle',
        'release_group',
    )

    for index, slot in enumerate(slots):
        if not isinstance(slot, dict):
            continue

        raw_id = str(slot.get('id') or f'slot_{index + 1}').strip()
        slot_id = re.sub(r'[^a-zA-Z0-9_-]+', '_', raw_id).strip('_') or f'slot_{index + 1}'
        name = str(slot.get('name') or f'版本槽位 {index + 1}').strip()
        suffix = str(slot.get('suffix') or name).strip()
        match = slot.get('match') if isinstance(slot.get('match'), dict) else {}
        clean_match = {}

        for field in list_fields:
            values = match.get(field)
            if not isinstance(values, list):
                values = [values] if values not in (None, '') else []
            clean_match[field] = list(dict.fromkeys(
                str(item).strip()
                for item in values
                if str(item or '').strip()
            ))

        clean_match['subtitle_effect'] = bool(match.get('subtitle_effect'))
        clean_match['clean_version'] = bool(match.get('clean_version'))
        clean_match['exempt_original_lang'] = bool(match.get('exempt_original_lang'))
        clean_match['min_size_gb'] = match.get('min_size_gb')
        clean_match['max_size_gb'] = match.get('max_size_gb')

        normalized.append({
            'id': slot_id,
            'name': name,
            'suffix': suffix,
            'fallback': bool(slot.get('fallback')),
            'match': clean_match,
        })

    return normalized


def _washing_json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value or '[]')
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _canonicalize_washing_rule_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonicalize_washing_rule_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if key not in {'_uid', '_group_name'}
        }
    if isinstance(value, list):
        normalized = [_canonicalize_washing_rule_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
        )
    return value


def _normalize_washing_priority_groups(groups: Any) -> list:
    normalized = []
    for group in groups if isinstance(groups, list) else []:
        if not isinstance(group, dict):
            continue
        media_type = str(group.get('media_type') or 'All').strip().title()
        if media_type not in {'All', 'Movie', 'Series'}:
            media_type = 'All'
        target_cids = sorted({
            str(cid).strip()
            for cid in _washing_json_list(group.get('target_cids'))
            if str(cid or '').strip()
        })
        priorities = [
            _canonicalize_washing_rule_value(rule)
            for rule in _washing_json_list(group.get('priorities'))
            if isinstance(rule, dict)
        ]
        normalized.append({
            'media_type': media_type,
            'target_cids': target_cids,
            'priorities': priorities,
        })
    return normalized


def _effective_washing_priorities(groups: list, media_type: str, target_cid: str) -> list:
    priorities = []
    for group in groups:
        if group['media_type'] not in {media_type, 'All'}:
            continue
        target_cids = group['target_cids']
        if target_cids and target_cid not in target_cids:
            continue
        priorities.extend(group['priorities'])
    return priorities


def build_washing_priority_recalculate_scope(old_groups: Any, new_groups: Any) -> Dict[str, Any]:
    """计算规则变更实际影响的媒体类型和分类 CID。"""
    old_normalized = _normalize_washing_priority_groups(old_groups)
    new_normalized = _normalize_washing_priority_groups(new_groups)
    explicit_cids = sorted({
        cid
        for group in old_normalized + new_normalized
        for cid in group['target_cids']
    })

    affected = {}
    unmatched_cid = '__etk_unmatched_washing_priority_cid__'
    for media_type in ('Movie', 'Series'):
        old_global = _effective_washing_priorities(old_normalized, media_type, unmatched_cid)
        new_global = _effective_washing_priorities(new_normalized, media_type, unmatched_cid)
        if old_global != new_global:
            affected[media_type] = {'all': True, 'target_cids': []}
            continue

        changed_cids = [
            cid
            for cid in explicit_cids
            if _effective_washing_priorities(old_normalized, media_type, cid)
            != _effective_washing_priorities(new_normalized, media_type, cid)
        ]
        if changed_cids:
            affected[media_type] = {'all': False, 'target_cids': changed_cids}
    return affected


def normalize_washing_priority_recalculate_scope(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    raw_scope = value.get('media_types') if isinstance(value.get('media_types'), dict) else value
    normalized = {}
    for media_type in ('Movie', 'Series'):
        entry = raw_scope.get(media_type)
        if not isinstance(entry, dict):
            continue
        is_all = bool(entry.get('all'))
        target_cids = sorted({
            str(cid).strip()
            for cid in _washing_json_list(entry.get('target_cids'))
            if str(cid or '').strip()
        })
        if is_all or target_cids:
            normalized[media_type] = {
                'all': is_all,
                'target_cids': [] if is_all else target_cids,
            }
    return normalized


def merge_washing_priority_recalculate_state(current: Any, added_scope: Any) -> Dict[str, Any]:
    current = current if isinstance(current, dict) else {}
    merged = normalize_washing_priority_recalculate_scope(current)
    added = normalize_washing_priority_recalculate_scope(added_scope)
    if not added:
        return {
            'revision': int(current.get('revision') or 0),
            'media_types': merged,
        }

    for media_type, entry in added.items():
        existing = merged.get(media_type, {'all': False, 'target_cids': []})
        if existing.get('all') or entry.get('all'):
            merged[media_type] = {'all': True, 'target_cids': []}
        else:
            merged[media_type] = {
                'all': False,
                'target_cids': sorted(set(existing.get('target_cids', [])) | set(entry.get('target_cids', []))),
            }

    return {
        'revision': int(current.get('revision') or 0) + 1,
        'media_types': merged,
    }


def get_washing_priority_recalculate_state() -> Dict[str, Any]:
    state = get_setting(WASHING_PRIORITY_RECALCULATE_SETTING_KEY) or {}
    if not isinstance(state, dict):
        state = {}
    return {
        'revision': int(state.get('revision') or 0),
        'media_types': normalize_washing_priority_recalculate_scope(state),
    }


def acknowledge_washing_priority_recalculate_scope(
    expected_revision: int,
    processed_scope: Any,
) -> bool:
    """任务成功后清除已处理范围；期间若规则再次变化则保留待处理状态。"""
    processed = normalize_washing_priority_recalculate_scope(processed_scope)
    if not processed:
        return False

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                (WASHING_PRIORITY_RECALCULATE_SETTING_KEY,),
            )
            row = cursor.fetchone()
            current = row.get('value_json') if row else {}
            current = current if isinstance(current, dict) else {}
            if int(current.get('revision') or 0) != int(expected_revision or 0):
                return False

            remaining = normalize_washing_priority_recalculate_scope(current)
            for media_type, entry in processed.items():
                existing = remaining.get(media_type)
                if not existing:
                    continue
                if entry.get('all'):
                    remaining.pop(media_type, None)
                    continue
                if existing.get('all'):
                    continue
                target_cids = sorted(
                    set(existing.get('target_cids', [])) - set(entry.get('target_cids', []))
                )
                if target_cids:
                    existing['target_cids'] = target_cids
                else:
                    remaining.pop(media_type, None)

            _save_setting_with_cursor(cursor, WASHING_PRIORITY_RECALCULATE_SETTING_KEY, {
                'revision': int(expected_revision or 0),
                'media_types': remaining,
            })
        conn.commit()
    return True

def get_washing_priority_config(default_conflict_mode: str = 'replace') -> Dict[str, Any]:
    """读取洗版相关配置，并从旧重命名配置无感迁移覆盖模式。"""
    default_mode = _normalize_washing_conflict_mode(default_conflict_mode, 'replace')
    config = get_setting('p115_washing_priority_config') or {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except Exception:
            config = {}
    if not isinstance(config, dict):
        config = {}

    mode = config.get('conflict_mode')
    if not mode:
        legacy = get_setting('p115_rename_config') or {}
        if isinstance(legacy, str):
            try:
                legacy = json.loads(legacy)
            except Exception:
                legacy = {}
        if isinstance(legacy, dict):
            mode = legacy.get('conflict_mode')

        if mode:
            config['conflict_mode'] = _normalize_washing_conflict_mode(mode, default_mode)
            try:
                save_setting('p115_washing_priority_config', config)
            except Exception:
                logger.warning("  ➜ 迁移洗版覆盖模式配置失败，将仅使用内存值。", exc_info=True)

    config['conflict_mode'] = _normalize_washing_conflict_mode(config.get('conflict_mode'), default_mode)
    config['skip_scope'] = _normalize_washing_skip_scope(config.get('skip_scope'), 'directory')
    config['version_slots_enabled'] = bool(config.get('version_slots_enabled'))
    config['version_slots'] = _normalize_washing_version_slots(config.get('version_slots'))
    return config

def save_washing_priority_config(value: Dict[str, Any]) -> Dict[str, Any]:
    config = value if isinstance(value, dict) else {}
    current = get_washing_priority_config()
    clean_config = {
        'conflict_mode': _normalize_washing_conflict_mode(config.get('conflict_mode'), 'replace'),
        'skip_scope': _normalize_washing_skip_scope(config.get('skip_scope'), 'directory'),
        'version_slots_enabled': bool(
            config.get('version_slots_enabled')
            if 'version_slots_enabled' in config
            else current.get('version_slots_enabled')
        ),
        'version_slots': _normalize_washing_version_slots(
            config.get('version_slots')
            if 'version_slots' in config
            else current.get('version_slots')
        ),
    }

    slots_changed = (
        bool(current.get('version_slots_enabled')) != clean_config['version_slots_enabled']
        or current.get('version_slots', []) != clean_config['version_slots']
    )

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            _save_setting_with_cursor(cursor, 'p115_washing_priority_config', clean_config)
            if slots_changed:
                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                    (WASHING_PRIORITY_RECALCULATE_SETTING_KEY,),
                )
                row = cursor.fetchone()
                current_state = row.get('value_json') if row else {}
                pending_state = merge_washing_priority_recalculate_state(current_state, {
                    'Movie': {'all': True, 'target_cids': []},
                    'Series': {'all': True, 'target_cids': []},
                })
                _save_setting_with_cursor(
                    cursor,
                    WASHING_PRIORITY_RECALCULATE_SETTING_KEY,
                    pending_state,
                )
        conn.commit()
    return clean_config

def get_washing_conflict_mode(default: str = 'replace') -> str:
    return get_washing_priority_config(default_conflict_mode=default).get('conflict_mode') or default

def get_washing_skip_scope(default: str = 'directory') -> str:
    return get_washing_priority_config().get('skip_scope') or default

def delete_setting(setting_key: str) -> bool:
    """从 app_settings 表中删除一个设置项。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM app_settings WHERE setting_key = %s", (setting_key,))
            conn.commit()
            logger.info(f"  ➜ 成功删除设置 '{setting_key}'。")
            return True
    except Exception as e:
        logger.error(f"  ➜ 删除设置 '{setting_key}' 时失败: {e}", exc_info=True)
        return False

# --- 全局订阅配额管理器 ---
def get_subscription_quota() -> int:
    """【V3 - 终极健壮版】获取当前可用的订阅配额。"""
    
    try:
        mp_config = get_setting('mp_config') or {}
        current_max_quota = mp_config.get('resubscribe_daily_cap', 200)
        today_str = datetime.now(pytz.timezone(constants.TIMEZONE)).strftime('%Y-%m-%d')

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            state = get_setting('subscription_quota_state') or {}
            last_reset_date = state.get('last_reset_date')
            
            if last_reset_date != today_str:
                logger.info(f"  ➜ 检测到新的一天 ({today_str})，正在重置订阅配额为 {current_max_quota}。")
                new_state = {
                    'current_quota': current_max_quota,
                    'last_reset_date': today_str,
                    'max_quota_on_reset': current_max_quota
                }
                save_setting('subscription_quota_state', new_state)
                return current_max_quota
            else:
                max_quota_on_reset = state.get('max_quota_on_reset', -1)
                current_quota_in_db = state.get('current_quota', 0)
                effective_remaining_quota = 0
                
                if max_quota_on_reset != -1 and current_max_quota != max_quota_on_reset:
                    consumed = max_quota_on_reset - current_quota_in_db
                    effective_remaining_quota = max(0, current_max_quota - consumed)
                else:
                    effective_remaining_quota = current_quota_in_db
                    
                final_remaining_quota = min(effective_remaining_quota, current_max_quota)
                
                if final_remaining_quota != current_quota_in_db or max_quota_on_reset == -1:
                    logger.info(f"  ➜ 动态调整或修正了当日订阅配额。旧剩余: {current_quota_in_db}, 新剩余: {final_remaining_quota}, 当前上限: {current_max_quota}")
                    new_state = {
                        'current_quota': final_remaining_quota,
                        'last_reset_date': today_str,
                        'max_quota_on_reset': current_max_quota
                    }
                    save_setting('subscription_quota_state', new_state)
                
                return final_remaining_quota

    except Exception as e:
        logger.error(f"获取订阅配额时发生严重错误，将返回0以确保安全: {e}", exc_info=True)
        return 0

def decrement_subscription_quota() -> bool:
    """将当前订阅配额减一。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                # 使用 FOR UPDATE 锁住这行，防止并发问题，这是很好的实践
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state' FOR UPDATE")
                row = cursor.fetchone()
                
                if not row or not row.get('value_json'):
                    # 注意：这里不需要 rollback，因为还没有做任何修改。
                    # 事务会在 with 块结束时自动处理。
                    logger.warning("  ➜ 尝试减少配额，但未找到配额状态记录。")
                    return False

                state = row['value_json']
                current_quota = state.get('current_quota', 0)

                if current_quota > 0:
                    state['current_quota'] = current_quota - 1
                    _save_setting_with_cursor(cursor, 'subscription_quota_state', state)
                    logger.debug(f"  ➜ 配额已消耗，剩余: {state['current_quota']}")
                
                # 所有操作成功，提交事务
                conn.commit()
                return True
            except Exception as e_trans:
                # 事务中发生任何错误，回滚
                conn.rollback()
                logger.error(f"  ➜ 减少配额的数据库事务失败: {e_trans}", exc_info=True)
                return False
    except Exception as e:
        logger.error(f"  ➜ 减少订阅配额时发生严重错误: {e}", exc_info=True)
        return False
    
def remove_item_from_recommendation_pool(tmdb_id: str):
    """
    从 'recommendation_pool' 列表中移除一个指定的媒体项。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = 'recommendation_pool' FOR UPDATE")
                result = cursor.fetchone()
                
                if not result or not result['value_json']:
                    logger.debug("  ➜ 推荐池为空或不存在，无需移除。")
                    return

                current_pool = result['value_json']
                new_pool = [item for item in current_pool if str(item.get('id')) != str(tmdb_id)]

                if len(new_pool) == len(current_pool):
                    logger.trace(f"  ➜ 尝试从推荐池移除 TMDB ID {tmdb_id}，但未在池中找到。")
                    return

                new_pool_json = json.dumps(new_pool, ensure_ascii=False)
                cursor.execute("""
                    UPDATE app_settings SET value_json = %s, last_updated_at = NOW()
                    WHERE setting_key = 'recommendation_pool'
                """, (new_pool_json,))
                
                # 添加下面这行来提交你的更改！
                conn.commit()
                
                logger.debug(f"  ➜ 已成功从推荐池中移除 TMDB ID: {tmdb_id}。")

    except Exception as e:
        # 发生错误时，数据库连接会自动回滚，所以这里不用显式 rollback
        logger.error(f"从推荐池移除 TMDB ID {tmdb_id} 时失败: {e}", exc_info=True)

# ======================================================================
# 模块: 共享资源独立配置
# ======================================================================
SHARED_RESOURCE_CONFIG_KEY = getattr(constants, 'APP_SETTING_SHARED_RESOURCE_CONFIG', 'shared_resource_config')

DEFAULT_SHARED_RESOURCE_CONFIG = {
    'p115_shared_resource_enabled': False,
    'p115_shared_center_url': 'https://shared.55565576.xyz',
    # 虚拟入库已移除：共享资源消费模式固定为 permanent。
    'p115_shared_resource_mode': 'permanent',
    'p115_shared_disable_episode_transfer': False,
    # 开启后，消费中心资源时会跳过被中心标记为“纯净版”的季包。
    'p115_shared_block_clean_version_transfer': False,
    # 开启后，消费中心资源时会跳过被中心标记为“短剧”的资源。
    'p115_shared_block_short_drama_transfer': False,
    # 开启后，才允许上传、拉取、合并共享片头章节。
    'p115_shared_intro_enabled': False,
    'p115_shared_auto_share_requests_enabled': False,
    'p115_shared_virtual_import_enabled': False,
    'p115_shared_virtual_auto_promote_episodes': 0,
    'p115_shared_virtual_auto_promote_movie_percent': 0,
    'p115_shared_center_home_sections': [],
}


def _shared_bool(value, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
    return bool(value)


def migrate_legacy_intro_detection_trigger_mode() -> bool:
    """将旧共享资源片头开关一次性迁移到智能追剧策略。"""
    legacy_config = get_setting(SHARED_RESOURCE_CONFIG_KEY) or {}
    if not isinstance(legacy_config, dict) or not _shared_bool(
        legacy_config.get('p115_etk_intro_detection_enabled'),
        False,
    ):
        return False

    watchlist_config = get_setting('watchlist_config') or {}
    watchlist_config = watchlist_config if isinstance(watchlist_config, dict) else {}
    if 'intro_detection_trigger_mode' in watchlist_config:
        return False

    migrated_config = dict(watchlist_config)
    migrated_config['intro_detection_trigger_mode'] = 'import'
    save_setting('watchlist_config', migrated_config)
    return True


def _shared_int(value, default: int = 0, minimum: int = None, maximum: int = None) -> int:
    try:
        if value in (None, ''):
            n = int(default)
        else:
            n = int(float(value))
    except Exception:
        n = int(default)
    if minimum is not None:
        n = max(int(minimum), n)
    if maximum is not None:
        n = min(int(maximum), n)
    return n


def _shared_center_home_sections(value) -> list:
    default_sections = [
        {'key': 'latest', 'title': '最新资源', 'display_type': 'all', 'order_by': 'pool_time', 'limit': 10, 'enabled': True},
        {'key': 'popular', 'title': '热门共享', 'display_type': 'all', 'order_by': 'popular', 'limit': 10, 'enabled': True},
        {'key': 'movies', 'title': '电影', 'display_type': 'movie', 'order_by': 'pool_time', 'limit': 10, 'enabled': True},
        {'key': 'series', 'title': '剧集', 'display_type': 'tv', 'order_by': 'pool_time', 'limit': 10, 'enabled': True},
    ]
    raw = value if isinstance(value, list) else default_sections
    out = []
    for index, item in enumerate(raw[:20]):
        if not isinstance(item, dict):
            continue
        key = str(item.get('key') or f'custom_{index + 1}').strip()[:64] or f'custom_{index + 1}'
        title = str(item.get('title') or key).strip()[:40] or key
        display_type = str(item.get('display_type') or item.get('item_type') or 'all').strip().lower()
        if display_type not in ('all', 'movie', 'tv', 'series', 'season', 'pack'):
            display_type = 'all'
        order_by = str(item.get('order_by') or 'latest').strip().lower()
        if order_by == 'latest':
            order_by = 'pool_time'
        if order_by not in ('pool_time', 'release_year', 'popular', 'size', 'name'):
            order_by = 'pool_time'
        genre_id = str(item.get('genre_id') or '').strip()[:40]
        tags = item.get('tags')
        if not isinstance(tags, list):
            tags = str(item.get('status') or '').split(',')
        tag_values = []
        for tag in tags:
            text = str(tag or '').strip()
            if re.fullmatch(r'[A-Za-z0-9_:-]{1,40}', text) and text not in tag_values:
                tag_values.append(text)
        out.append({
            'key': key,
            'title': title,
            'display_type': display_type,
            'order_by': order_by,
            'status': ','.join(['alive', 'available', *tag_values]),
            'genre_id': genre_id,
            'tags': tag_values,
            'limit': _shared_int(item.get('limit'), 10, 1, 20),
            'enabled': _shared_bool(item.get('enabled'), True),
        })
    return out or default_sections


def normalize_shared_resource_config(value: Optional[Dict[str, Any]] = None, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """规范化共享资源配置。

    注意：共享资源配置是独立 app_settings 记录，不再读写 dynamic_app_config / APP_CONFIG。
    """
    merged = dict(DEFAULT_SHARED_RESOURCE_CONFIG)
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(value, dict):
        merged.update(value)

    center_url = str(merged.get('p115_shared_center_url') or DEFAULT_SHARED_RESOURCE_CONFIG['p115_shared_center_url']).strip().rstrip('/')
    if not center_url:
        center_url = DEFAULT_SHARED_RESOURCE_CONFIG['p115_shared_center_url']
    # 虚拟入库已移除，旧配置里即便残留 virtual/cache/auto_promote，也不再回写。
    mode = 'permanent'

    return {
        'p115_shared_resource_enabled': _shared_bool(merged.get('p115_shared_resource_enabled'), False),
        'p115_shared_center_url': center_url,
        'p115_shared_resource_mode': mode,
        'p115_shared_disable_episode_transfer': _shared_bool(merged.get('p115_shared_disable_episode_transfer'), False),
        'p115_shared_block_clean_version_transfer': _shared_bool(
            merged.get('p115_shared_block_clean_version_transfer', merged.get('shared_block_clean_version_transfer')),
            False,
        ),
        'p115_shared_block_short_drama_transfer': _shared_bool(
            merged.get('p115_shared_block_short_drama_transfer', merged.get('shared_block_short_drama_transfer')),
            False,
        ),
        'p115_shared_intro_enabled': _shared_bool(merged.get('p115_shared_intro_enabled'), False),
        'p115_shared_auto_share_requests_enabled': _shared_bool(merged.get('p115_shared_auto_share_requests_enabled'), False),
        'p115_shared_virtual_import_enabled': _shared_bool(merged.get('p115_shared_virtual_import_enabled'), False),
        'p115_shared_virtual_auto_promote_episodes': _shared_int(merged.get('p115_shared_virtual_auto_promote_episodes'), 0, 0),
        'p115_shared_virtual_auto_promote_movie_percent': _shared_int(merged.get('p115_shared_virtual_auto_promote_movie_percent'), 0, 0, 100),
        'p115_shared_center_home_sections': _shared_center_home_sections(merged.get('p115_shared_center_home_sections')),
    }


def get_shared_resource_config() -> Dict[str, Any]:
    """读取共享资源独立配置。"""
    data = get_setting(SHARED_RESOURCE_CONFIG_KEY) or {}
    return normalize_shared_resource_config(data if isinstance(data, dict) else {})


def save_shared_resource_config(value: Dict[str, Any]) -> Dict[str, Any]:
    """保存共享资源独立配置，并返回规范化后的完整配置。"""
    # 规范化会移除已废弃的旧开关，先迁移避免用户先保存共享配置而丢失启用状态。
    migrate_legacy_intro_detection_trigger_mode()
    current = get_shared_resource_config()
    payload = normalize_shared_resource_config(value if isinstance(value, dict) else {}, base=current)
    save_setting(SHARED_RESOURCE_CONFIG_KEY, payload)
    return payload
