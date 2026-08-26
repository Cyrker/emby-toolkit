from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from pathlib import PurePath
from typing import Any, cast

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .directory_sync import P115DirectoryTreeClient
from .episode_regex import match_episode_regex
from .models import P115AuthorizationPoll, P115AuthorizationSession, P115CredentialSnapshot
from .organize_execution import P115OrganizeMutationClient, P115RemoteEntry
from .organize_plan import (
    P115PlanNamingGateway,
    P115PlanRecognitionGateway,
    P115SourceNode,
)
from .rename_config import P115RenamePreviewGateway, P115WashingPriorityConfigGateway
from .sorting_rules import P115SortingRuleDirectoryGateway

_OPENAPI_DEVICE_CODE_URL = "https://passportapi.115.com/open/authDeviceCode"
_OPENAPI_TOKEN_URL = "https://passportapi.115.com/open/deviceCodeToToken"
_OPENAPI_USER_URL = "https://proapi.115.com/open/user/info"
_QRCODE_STATUS_URL = "https://qrcodeapi.115.com/get/status/"
_COOKIE_FILES_URL = "https://webapi.115.com/files"

_APP_LABELS = {
    "web": "网页版",
    "tv": "安卓电视端",
    "alipaymini": "支付宝小程序",
    "wechatmini": "微信小程序",
}

_APP_USER_AGENTS = {
    "web": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "mac": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2",
    "linux": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2",
    "tv": (
        "Mozilla/5.0 (Linux; Android 7.1.2; 115disk Build/NHG47K; wv) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.114 Safari/537.36"
    ),
    "alipaymini": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Mobile/15E148 AlipayDefined(nt:WIFI,ws:390|844|3.0) "
        "AliApp(AP/10.5.33.8143) AlipayClient/10.5.33.8143 Language/zh-Hans Region/CN"
    ),
    "wechatmini": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.42(0x18002a2c) "
        "NetType/WIFI Language/zh_CN"
    ),
}


class P115ProviderError(RuntimeError):
    pass


class HttpxP115AuthorizationGateway:
    """Bounded 115 authorization/status adapter with injectable local HTTP transport."""

    def __init__(
        self,
        *,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory
        self._clock = clock

    def create_openapi_qrcode(self, client_id: str) -> tuple[dict[str, Any], P115AuthorizationSession]:
        verifier = base64.urlsafe_b64encode(secrets.token_bytes(40)).decode("ascii").rstrip("=")
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).decode("ascii").rstrip("=")
        )
        payload, _cookies = self._request_json(
            "POST",
            _OPENAPI_DEVICE_CODE_URL,
            data={
                "client_id": client_id,
                "code_challenge": challenge,
                "code_challenge_method": "sha256",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        data = _success_data(payload)
        session = P115AuthorizationSession(
            uid=str(data.get("uid") or ""),
            time_value=str(data.get("time") or ""),
            sign=str(data.get("sign") or ""),
            code_verifier=verifier,
            created_at=self._clock(),
        )
        if not session.uid:
            raise P115ProviderError("获取二维码失败")
        return data, session

    def poll_openapi_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll:
        status_payload, _cookies = self._request_json(
            "GET",
            _QRCODE_STATUS_URL,
            params={"uid": session.uid, "time": session.time_value, "sign": session.sign},
            retryable=True,
            timeout=30.0,
        )
        state = status_payload.get("state")
        if state == 0:
            return P115AuthorizationPoll("expired", "二维码已过期，请重新获取")
        status = _status_value(status_payload)
        if state == 1 and status == 1:
            return P115AuthorizationPoll("waiting", "已扫码，等待手机端确认...")
        if state != 1 or status != 2:
            return P115AuthorizationPoll("waiting", "等待扫码...")

        token_payload, _cookies = self._request_json(
            "POST",
            _OPENAPI_TOKEN_URL,
            data={"uid": session.uid, "code_verifier": session.code_verifier},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if not token_payload.get("state"):
            return P115AuthorizationPoll(
                "error",
                f"获取Token失败: {token_payload.get('message', '未知错误')}",
            )
        token_data = _string_mapping(token_payload.get("data"))
        access_token = str(token_data.get("access_token") or "").strip()
        refresh_token = str(token_data.get("refresh_token") or "").strip()
        if not access_token:
            return P115AuthorizationPoll("error", "获取Token失败: 缺少 access_token")
        user_payload, _cookies = self._request_json(
            "GET",
            _OPENAPI_USER_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            retryable=True,
        )
        user_info = _string_mapping(user_payload.get("data"))
        return P115AuthorizationPoll(
            "success",
            "登录成功",
            user_info=user_info,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    def create_cookie_qrcode(self, app_type: str) -> tuple[dict[str, Any], P115AuthorizationSession]:
        app = _app_type(app_type)
        payload, _cookies = self._request_json(
            "GET",
            f"https://qrcodeapi.115.com/api/1.0/{app}/1.0/token/",
            params={"app": app},
            headers={"User-Agent": _user_agent(app)},
            retryable=True,
        )
        data = _success_data(payload)
        session = P115AuthorizationSession(
            uid=str(data.get("uid") or ""),
            time_value=str(data.get("time") or ""),
            sign=str(data.get("sign") or ""),
            app_type=app,
            created_at=self._clock(),
        )
        if not session.uid:
            raise P115ProviderError("获取失败")
        return data, session

    def poll_cookie_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll:
        headers = {"User-Agent": _user_agent(session.app_type)}
        payload, _cookies = self._request_json(
            "GET",
            _QRCODE_STATUS_URL,
            params={"uid": session.uid, "time": session.time_value, "sign": session.sign},
            headers=headers,
            retryable=True,
        )
        state = payload.get("state")
        if state == 0:
            return P115AuthorizationPoll("expired", "二维码已过期")
        status = _status_value(payload)
        if state == 1 and status == 1:
            return P115AuthorizationPoll("waiting", "已扫码，请在手机端确认")
        if state != 1 or status != 2:
            return P115AuthorizationPoll("waiting", "等待扫码...")
        login_payload, response_cookies = self._request_json(
            "POST",
            f"https://passportapi.115.com/app/1.0/{session.app_type}/1.0/login/qrcode",
            data={"account": session.uid, "app": session.app_type},
            headers=headers,
        )
        if login_payload.get("state") != 1:
            return P115AuthorizationPoll("error", str(login_payload.get("message") or "登录失败"))
        cookie = "; ".join(f"{key}={value}" for key, value in response_cookies.items())
        if not cookie:
            return P115AuthorizationPoll("error", "登录失败")
        return P115AuthorizationPoll(
            "success",
            "Cookie 获取成功！",
            cookie=cookie,
            app_type=session.app_type,
        )

    def check_openapi(self, access_token: str) -> Mapping[str, Any]:
        payload, _cookies = self._request_json(
            "GET",
            _OPENAPI_USER_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            retryable=True,
        )
        return payload

    def check_cookie(self, cookie: str, app_type: str) -> Mapping[str, Any]:
        payload, _cookies = self._request_json(
            "GET",
            _COOKIE_FILES_URL,
            params={"cid": 0, "limit": 1},
            headers={"User-Agent": _user_agent(app_type), "Cookie": cookie},
            retryable=True,
        )
        return payload

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        retryable: bool = False,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        def invoke() -> tuple[dict[str, Any], dict[str, str]]:
            with self._client_factory(timeout=timeout, follow_redirects=True) as client:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, Mapping):
                    raise P115ProviderError("115 返回了无效 JSON")
                return dict(payload), dict(response.cookies.items())

        if not retryable:
            return invoke()
        retrying = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.1, max=1.0),
            retry=retry_if_exception_type(httpx.TransportError),
            reraise=True,
        )
        return retrying(invoke)


class LegacyP115CredentialStore:
    """P11 compatibility adapter around the existing atomic credential record."""

    def load(self) -> P115CredentialSnapshot:
        from handler.p115_service import get_115_account_id, get_115_tokens

        access_token, refresh_token, cookie, app_type = get_115_tokens()
        return P115CredentialSnapshot(
            access_token=str(access_token or "").strip(),
            refresh_token=str(refresh_token or "").strip(),
            cookie=str(cookie or "").strip(),
            app_type=str(app_type or "web").strip() or "web",
            account_id=str(get_115_account_id() or "").strip(),
        )

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None:
        from handler.p115_service import save_115_tokens

        save_115_tokens(
            access_token,
            refresh_token,
            account_id=account_id,
            credential_type="openapi",
            expected_access_token=expected_access_token,
        )

    def save_cookie(self, cookie: str, app_type: str) -> None:
        from handler.p115_service import save_115_tokens

        save_115_tokens(
            None,
            None,
            cookie,
            app_type,
            credential_type="cookie",
        )

    def app_label(self, app_type: str) -> str:
        return _APP_LABELS.get(_app_type(app_type), str(app_type or "web"))


class LegacyP115DirectoryGateway:
    """P11 compatibility adapter around the split OpenAPI/Cookie directory client."""

    def list_entries(
        self,
        *,
        cid: int,
        search: str,
        expected_account_id: str,
    ) -> Mapping[str, Any] | None:
        from handler.p115_service import P115Service

        client = P115Service.get_client(expected_account_id=expected_account_id or None)
        if client is None:
            return None
        payload: dict[str, Any] = {"cid": cid, "limit": 1000}
        if search:
            payload["search_value"] = search
            return client.fs_search(payload)
        return client.fs_files(payload)


class LegacyP115DirectoryTreeGateway:
    """Open an account-bound compatibility client for authoritative directory pages."""

    def open(self, *, expected_account_id: str) -> P115DirectoryTreeClient | None:
        from handler.p115_service import P115Service

        account_id = str(expected_account_id or "").strip()
        if not account_id:
            return None
        client = P115Service.get_client(expected_account_id=account_id)
        if client is None:
            return None
        return _LegacyP115DirectoryTreeClient(client, account_id)


class LegacyP115OrganizeMutationGateway:
    """Bind one account and one low-level backend for a complete P6.4 execution."""

    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> P115OrganizeMutationClient | None:
        from handler.p115_service import P115Service, get_115_account_id

        account_id = str(expected_account_id or "").strip()
        if not account_id or get_115_account_id() != account_id:
            return None
        client = P115Service.get_client(expected_account_id=account_id)
        if client is None or not hasattr(client, "p115_organize_backends"):
            return None
        available = tuple(str(value).strip().lower() for value in client.p115_organize_backends())
        requested = str(expected_backend or "").strip().lower()
        if requested:
            if requested not in available:
                return None
            backend = requested
        else:
            backend = next((value for value in available if value in {"openapi", "cookie"}), "")
        if not backend:
            return None
        return _LegacyP115OrganizeMutationClient(client, account_id, backend)


class LegacyP115SortingRuleDirectoryGateway(P115SortingRuleDirectoryGateway):
    """Resolve a rule CID path through the account-bound read-only compatibility client."""

    def is_available(self, *, expected_account_id: str) -> bool:
        from handler.p115_service import P115Service, get_115_account_id

        if get_115_account_id() != expected_account_id:
            raise RuntimeError("115 账号在分类规则路径解析前发生变化")
        return P115Service.get_client(expected_account_id=expected_account_id) is not None

    def resolve_path(
        self,
        *,
        cid: str,
        expected_account_id: str,
    ) -> tuple[Mapping[str, Any], ...] | None:
        from handler.p115_service import P115Service, get_115_account_id, p115_expected_account

        if get_115_account_id() != expected_account_id:
            raise RuntimeError("115 账号在分类规则路径解析前发生变化")
        client = P115Service.get_client(expected_account_id=expected_account_id)
        if client is None:
            return None
        with p115_expected_account(expected_account_id):
            response = client.fs_files(
                {
                    "cid": cid,
                    "limit": 1,
                    "record_open_time": 0,
                    "count_folders": 0,
                }
            )
        if not isinstance(response, Mapping) or not response.get("state"):
            return None
        path = response.get("path")
        if not isinstance(path, list) or any(not isinstance(node, Mapping) for node in path):
            return None
        return tuple(cast(Mapping[str, Any], node) for node in path)


class LegacyP115RenamePreviewGateway(P115RenamePreviewGateway):
    """Render fixed compatibility samples without provider or persistence access."""

    def preview(
        self,
        *,
        config: Mapping[str, Any],
        templates: Mapping[str, Any],
    ) -> Mapping[str, str]:
        from handler.p115_rename import P115RenameRenderer

        movie = _rename_preview_payload(is_tv=False)
        tv = _rename_preview_payload(is_tv=True)
        customization_rules = config.get("customization", P115RenameRenderer.DEFAULT_CUSTOMIZATION)
        for context in (movie, tv):
            customization = P115RenameRenderer.extract_customization(
                context["original_name"],
                customization_rules,
            )
            context["video_info"]["customization"] = customization
            context["video_info"]["stream"] = customization

        movie_renderer = P115RenameRenderer(
            movie["details"],
            movie["tmdb_id"],
            movie["original_title"],
            dict(config),
        )
        tv_renderer = P115RenameRenderer(
            tv["details"],
            tv["tmdb_id"],
            tv["original_title"],
            dict(config),
        )
        movie_dir = movie_renderer.render_template(
            templates.get("main_dir_template") or "",
            original_title=movie["original_title"],
            original_name=movie["original_name"],
            video_info=movie["video_info"],
            safe_title=movie["safe_title"],
            file_ext=movie["file_ext"],
        )
        tv_render_options = {
            "is_tv": True,
            "season_num": tv["season_num"],
            "episode_num": tv["episode_num"],
            "original_title": tv["original_title"],
            "original_name": tv["original_name"],
            "video_info": tv["video_info"],
            "safe_title": tv["safe_title"],
            "file_ext": tv["file_ext"],
        }
        tv_dir = tv_renderer.render_template(
            templates.get("main_dir_template") or "",
            **tv_render_options,
        )
        tv_season = tv_renderer.render_template(
            templates.get("season_dir_template") or "",
            **tv_render_options,
        )
        movie_file = (
            movie["original_name"]
            if config.get("keep_original_name")
            else movie_renderer.render_template(
                templates.get("movie_file_template") or "",
                original_title=movie["original_title"],
                original_name=movie["original_name"],
                video_info=movie["video_info"],
                safe_title=movie["safe_title"],
                file_ext=movie["file_ext"],
            )
        )
        tv_file = (
            tv["original_name"]
            if config.get("keep_original_name")
            else tv_renderer.render_template(
                templates.get("tv_file_template") or templates.get("file_template") or "",
                **tv_render_options,
            )
        )
        return {
            "movie_dir": movie_dir or "未配置主目录规则",
            "tv_dir": tv_dir or "未配置主目录规则",
            "tv_season": tv_season or "未配置季目录规则",
            "movie_file": movie_file or movie["original_name"],
            "tv_file": tv_file or tv["original_name"],
        }


class LegacyP115WashingPriorityConfigGateway(P115WashingPriorityConfigGateway):
    """Retain the legacy conflict-mode split until the P7 washing migration."""

    def save_conflict_mode(self, conflict_mode: object) -> None:
        from database import settings_db

        settings_db.save_washing_priority_config({"conflict_mode": conflict_mode})


def _policy_list(
    policy: Mapping[str, Any],
    key: str,
    fallback: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    configured = policy.get(key)
    values = configured if isinstance(configured, list) else fallback
    return tuple(cast(Mapping[str, Any], value) for value in values if isinstance(value, Mapping))


def _enrich_rule_dimensions(
    recognition: dict[str, Any],
    *,
    country_mapping: Sequence[Mapping[str, Any]],
    language_mapping: Sequence[Mapping[str, Any]],
    studio_mapping: Sequence[Mapping[str, Any]],
    keyword_mapping: Sequence[Mapping[str, Any]],
) -> None:
    country_values = _value_set(recognition.get("country_codes") or recognition.get("countries"))
    recognition["countries"] = _mapped_labels(
        recognition.get("countries"),
        country_values,
        country_mapping,
        value_keys=("value", "aliases"),
    )
    language_values = _value_set(recognition.get("lang_code") or recognition.get("languages"))
    recognition["languages"] = _mapped_labels(
        recognition.get("languages"),
        language_values,
        language_mapping,
        value_keys=("value", "aliases"),
    )
    studio_values = _value_set(recognition.get("company_ids")) | _value_set(recognition.get("network_ids"))
    recognition["studios"] = _mapped_labels(
        recognition.get("studios"),
        studio_values,
        studio_mapping,
        value_keys=("company_ids", "network_ids"),
    )
    keyword_values = _value_set(recognition.get("keyword_ids"))
    recognition["keywords"] = _mapped_labels(
        recognition.get("keywords"),
        keyword_values,
        keyword_mapping,
        value_keys=("ids",),
    )


def _mapped_labels(
    existing: object,
    observed: set[str],
    mapping: Sequence[Mapping[str, Any]],
    *,
    value_keys: Sequence[str],
) -> list[Any]:
    values = list(existing) if isinstance(existing, Sequence) and not isinstance(existing, (str, bytes)) else []
    seen = _value_set(values)
    for item in mapping:
        candidates: set[str] = set()
        for key in value_keys:
            candidates.update(_value_set(item.get(key)))
        label = item.get("label")
        if label not in (None, "") and observed & candidates and str(label).casefold() not in seen:
            values.append(deepcopy(label))
            seen.add(str(label).casefold())
    return values


def _value_set(value: object) -> set[str]:
    values = value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else [value]
    return {str(item).strip().casefold() for item in values if item not in (None, "")}


class LegacyP115RuleRecognitionGateway(P115PlanRecognitionGateway):
    """Use only the legacy pure rule parser; never construct SmartOrganizer."""

    def recognize(
        self,
        node: P115SourceNode,
        *,
        hints: Mapping[str, Any],
        episode_rules: Sequence[Mapping[str, Any]],
        policy: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        import utils
        from handler.p115_service import _build_rule_parse_result

        parent_name = node.path.rsplit("/", 2)[-2] if "/" in node.path else ""
        forced_type = str(hints.get("media_type") or "").strip() or None
        result = deepcopy(
            _build_rule_parse_result(
                filename=node.name,
                main_dir_name=parent_name,
                has_season_subdirs=False,
                forced_media_type=forced_type,
                is_folder=False,
            )
        )
        custom_episode = match_episode_regex(
            episode_rules,
            node.name,
            relative_path=node.path,
        )
        evidence = [str(value) for value in result.get("evidence") or []]
        if custom_episode is not None:
            result["media_type"] = "tv"
            result["season_number"] = custom_episode.season
            result["episode_number"] = custom_episode.episode
            result["custom_episode_rule"] = custom_episode.rule_name
            evidence.append("custom_episode_regex")
        raw_details = hints.get("details")
        details: Mapping[str, Any] = cast(Mapping[str, Any], raw_details) if isinstance(raw_details, Mapping) else {}
        for key in (
            "actor_ids",
            "company_ids",
            "countries",
            "country_codes",
            "genre_ids",
            "keyword_ids",
            "keywords",
            "lang_code",
            "languages",
            "network_ids",
            "rating_label",
            "runtime",
            "studios",
            "vote_average",
            "watching_status",
        ):
            if key in hints:
                result[key] = deepcopy(hints[key])
            elif key in details:
                result[key] = deepcopy(details[key])
        for key in (
            "episode_number",
            "media_type",
            "season_number",
            "title",
            "tmdb_id",
            "year",
        ):
            if hints.get(key) not in (None, ""):
                result[key] = deepcopy(hints[key])
                evidence.append(f"explicit_hint:{key}")
        if details:
            result["details"] = deepcopy(dict(details))
        if isinstance(hints.get("video_info"), Mapping):
            result["video_info"] = deepcopy(dict(cast(Mapping[str, Any], hints["video_info"])))
        _enrich_rule_dimensions(
            result,
            country_mapping=_policy_list(policy, "country_mapping", utils.DEFAULT_COUNTRY_MAPPING),
            language_mapping=_policy_list(policy, "language_mapping", utils.DEFAULT_LANGUAGE_MAPPING),
            studio_mapping=_policy_list(policy, "studio_mapping", utils.DEFAULT_STUDIO_MAPPING),
            keyword_mapping=_policy_list(policy, "keyword_mapping", utils.DEFAULT_KEYWORD_MAPPING),
        )
        result["evidence"] = list(dict.fromkeys(evidence))
        return cast(Mapping[str, Any], result)


class LegacyP115PlanNamingGateway(P115PlanNamingGateway):
    """Render names through the pure compatibility renderer without any I/O."""

    def render_names(
        self,
        node: P115SourceNode,
        *,
        recognition: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> Mapping[str, str]:
        from handler.p115_rename import P115RenameRenderer

        raw_details = recognition.get("details")
        details = dict(raw_details) if isinstance(raw_details, Mapping) else {}
        title = str(recognition.get("title") or details.get("title") or "").strip()
        year = str(recognition.get("year") or "").strip()
        details.setdefault("title", title)
        if year and not details.get("date"):
            details["date"] = f"{year}-01-01"
        original_title = str(
            recognition.get("original_title") or details.get("original_title") or details.get("title_en") or title
        )
        renderer = P115RenameRenderer(
            details,
            str(recognition.get("tmdb_id") or ""),
            original_title,
            dict(config),
        )
        is_tv = recognition.get("media_type") == "tv"
        season_number = recognition.get("season_number")
        episode_number = recognition.get("episode_number")
        suffix = PurePath(node.name).suffix
        video_info = dict(recognition["video_info"]) if isinstance(recognition.get("video_info"), Mapping) else {}
        options = {
            "is_tv": is_tv,
            "season_num": season_number,
            "episode_num": episode_number,
            "original_title": original_title,
            "original_name": node.name,
            "video_info": video_info,
            "safe_title": title,
            "file_ext": suffix,
        }
        main_dir = renderer.render_template(config.get("main_dir_template") or "", **options) or title
        season_dir = ""
        if is_tv:
            season_dir = renderer.render_template(config.get("season_dir_template") or "", **options)
            if not season_dir and season_number is not None:
                season_dir = f"Season {int(season_number):02d}"
        if config.get("keep_original_name"):
            file_name = node.name
        else:
            template_key = "tv_file_template" if is_tv else "movie_file_template"
            template = config.get(template_key) or config.get("file_template") or ""
            file_name = renderer.render_template(template, **options) or node.name
        return {"main_dir": main_dir, "season_dir": season_dir, "file_name": file_name}


class _LegacyP115DirectoryTreeClient:
    def __init__(self, client: Any, account_id: str) -> None:
        self._client = client
        self._account_id = account_id
        self._api_label: str | None = None

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        from handler.p115_service import get_115_account_id, p115_expected_account

        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在目录同步期间发生变化")
        with p115_expected_account(self._account_id):
            response = self._client.fs_files(
                {
                    "cid": cid,
                    "limit": limit,
                    "offset": offset,
                    "record_open_time": 0,
                    "count_folders": 0,
                }
            )
        if not isinstance(response, Mapping):
            raise RuntimeError("115 目录树分页返回了无效响应结构")
        if response.get("state") is True and response.get("_etk_authoritative_list_payload") is not True:
            raise RuntimeError("115 目录树分页缺少权威列表载荷")
        if response.get("state") is True:
            api_label = (
                str(response.get("_etk_api_label_lower") or response.get("_etk_api_label") or "").strip().lower()
            )
            if not api_label:
                raise RuntimeError("115 目录树分页缺少稳定 API 后端标识")
            if self._api_label is None:
                self._api_label = api_label
            elif api_label != self._api_label:
                raise RuntimeError(f"115 目录树分页 API 后端发生变化：{self._api_label} -> {api_label}")
        return cast(Mapping[str, Any], response)


class _LegacyP115OrganizeMutationClient:
    _PAGE_LIMIT = 1000
    _MAX_PAGES = 1000

    def __init__(self, client: Any, account_id: str, backend: str) -> None:
        self._client = client
        self._account_id = account_id
        self._backend = backend

    @property
    def backend(self) -> str:
        return self._backend

    def list_children(self, parent_id: str) -> Sequence[P115RemoteEntry]:
        parent = str(parent_id or "").strip()
        if not parent:
            raise RuntimeError("115 权威目录回读缺少 parent_id")
        offset = 0
        received = 0
        reported_total: int | None = None
        pages = 0
        entries: list[P115RemoteEntry] = []
        seen_ids: set[str] = set()
        seen_pages: set[str] = set()
        while True:
            response = self._call(
                "fs_files_backend",
                {
                    "cid": parent,
                    "limit": self._PAGE_LIMIT,
                    "offset": offset,
                    "record_open_time": 0,
                    "count_folders": 0,
                },
                self._backend,
            )
            if response.get("state") is not True:
                raise RuntimeError(str(response.get("message") or response.get("error_msg") or "115 目录回读失败"))
            label = str(response.get("_etk_api_label_lower") or "").strip().lower()
            if label != self._backend:
                raise RuntimeError("115 整理回读后端与固定 mutation 后端不一致")
            if response.get("_etk_authoritative_list_payload") is not True:
                raise RuntimeError("115 整理回读缺少权威列表载荷")
            data = response.get("data")
            if not isinstance(data, list) or any(not isinstance(item, Mapping) for item in data):
                raise RuntimeError("115 整理回读返回了无效目录列表")
            if len(data) > self._PAGE_LIMIT:
                raise RuntimeError("115 整理回读页面超过请求上限")
            total = _optional_nonnegative_count(response.get("count"))
            if total is not None:
                if reported_total is None:
                    reported_total = total
                elif reported_total != total:
                    raise RuntimeError("115 整理回读分页总数发生变化")
            if not data:
                if reported_total is not None and received != reported_total:
                    raise RuntimeError("115 整理回读分页不完整")
                break
            if pages >= self._MAX_PAGES:
                raise RuntimeError("115 整理回读分页超过安全上限")
            pages += 1
            page_hash = _stable_json_hash([dict(cast(Mapping[str, Any], item)) for item in data])
            if page_hash in seen_pages:
                raise RuntimeError("115 整理回读重复返回相同页面")
            seen_pages.add(page_hash)
            for item in data:
                entry = _p115_remote_entry(parent, cast(Mapping[str, Any], item))
                if entry.id in seen_ids:
                    raise RuntimeError(f"115 整理回读重复返回项目 ID: {entry.id}")
                seen_ids.add(entry.id)
                entries.append(entry)
            received += len(data)
            if reported_total is not None and received > reported_total:
                raise RuntimeError("115 整理回读数量超过 provider count")
            offset += len(data)
        return tuple(entries)

    def ensure_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        return self._call("fs_mkdir_backend", str(name), str(parent_id), self._backend)

    def move(self, file_id: str, parent_id: str) -> Mapping[str, Any]:
        return self._call("fs_move_backend", [str(file_id)], str(parent_id), self._backend)

    def rename(self, file_id: str, name: str) -> Mapping[str, Any]:
        return self._call("fs_rename_backend", (str(file_id), str(name)), self._backend)

    def _call(self, method_name: str, *args: Any) -> Mapping[str, Any]:
        from handler.p115_service import get_115_account_id, p115_expected_account

        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在整理执行期间发生变化")
        method = getattr(self._client, method_name, None)
        if not callable(method):
            raise RuntimeError(f"115 固定后端缺少 {method_name}")
        with p115_expected_account(self._account_id):
            response = method(*args)
        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在整理请求返回后发生变化")
        if not isinstance(response, Mapping):
            raise RuntimeError("115 固定后端返回了无效响应结构")
        return cast(Mapping[str, Any], response)


def _p115_remote_entry(parent_id: str, item: Mapping[str, Any]) -> P115RemoteEntry:
    item_id = str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()
    name = str(item.get("fn") or item.get("file_name") or item.get("n") or item.get("name") or "").strip()
    if not item_id or not name:
        raise RuntimeError("115 整理回读项目缺少 ID 或名称")
    category = item.get("fc") if item.get("fc") is not None else item.get("type")
    if str(category) == "0":
        entry_kind = "directory"
    elif str(category) == "1" or item.get("sha") or item.get("sha1") or item.get("pc"):
        entry_kind = "file"
    else:
        raise RuntimeError("115 整理回读项目类型未知")
    size = _optional_nonnegative_count(item.get("fs") if item.get("fs") is not None else item.get("size")) or 0
    normalized = {
        "id": item_id,
        "parent_id": parent_id,
        "name": name,
        "entry_kind": entry_kind,
        "sha1": str(item.get("sha") or item.get("sha1") or "").strip().upper(),
        "pick_code": str(item.get("pc") or item.get("pick_code") or "").strip(),
        "size": size,
        "updated_at": str(item.get("te") or item.get("upt") or item.get("updated_at") or "").strip(),
    }
    return P115RemoteEntry(**normalized, raw_hash=_stable_json_hash(normalized))


def _optional_nonnegative_count(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise RuntimeError("115 provider 返回了无效 count")
    parsed = int(str(value))
    if parsed < 0:
        raise RuntimeError("115 provider 返回了负数 count")
    return parsed


def _stable_json_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _success_data(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload.get("state"):
        raise P115ProviderError(str(payload.get("message") or "获取失败"))
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise P115ProviderError("115 返回了无效数据")
    return dict(data)


def _status_value(payload: Mapping[str, Any]) -> int | None:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    try:
        return int(str(data.get("status")))
    except (TypeError, ValueError):
        return None


def _app_type(value: str) -> str:
    normalized = str(value or "web").strip().lower()
    return normalized or "web"


def _user_agent(app_type: str) -> str:
    return _APP_USER_AGENTS.get(_app_type(app_type), _APP_USER_AGENTS["web"])


def _string_mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _rename_preview_payload(*, is_tv: bool) -> dict[str, Any]:
    if is_tv:
        return {
            "details": {
                "title": "绝命毒师",
                "title_en": "Breaking Bad",
                "original_title": "Breaking Bad",
                "date": "2008-01-20",
            },
            "tmdb_id": "1396",
            "original_title": "Breaking Bad",
            "safe_title": "绝命毒师",
            "original_name": "Breaking.Bad.S01E01.2160p.NF.WEB-DL.HDR.60fps.HEVC.Atmos-HHWEB.mp4",
            "season_num": 1,
            "episode_num": 1,
            "file_ext": ".mp4",
            "video_info": {
                "resolution": "2160p",
                "source": "WEB-DL",
                "stream": "friDay",
                "customization": "friDay",
                "effect": "HDR",
                "codec": "HEVC",
                "videoCodec": "HEVC",
                "videoBit": "10bit",
                "audio_count": "2Audios",
                "audio": "Atmos",
                "audioCodec": "Atmos",
                "fps": "60",
                "group": "HHWEB",
            },
        }
    return {
        "details": {
            "title": "寄生虫",
            "title_en": "Parasite",
            "original_title": "기생충",
            "date": "2019-05-30",
        },
        "tmdb_id": "496243",
        "original_title": "Parasite",
        "safe_title": "寄生虫",
        "original_name": "Parasite.2019.60fps.1080p.BluRay.HDR.AVC.DDP5.1-CMCT.mkv",
        "season_num": None,
        "episode_num": None,
        "file_ext": ".mkv",
        "video_info": {
            "resolution": "1080p",
            "source": "BluRay",
            "stream": "60fps",
            "customization": "60fps",
            "effect": "HDR",
            "codec": "AVC",
            "videoCodec": "AVC",
            "videoBit": "8bit",
            "audio_count": "2Audios",
            "audio": "DDP 5.1",
            "audioCodec": "DDP 5.1",
            "fps": "23.976",
            "group": "CMCT",
        },
    }


__all__ = [
    "HttpxP115AuthorizationGateway",
    "LegacyP115CredentialStore",
    "LegacyP115DirectoryGateway",
    "LegacyP115DirectoryTreeGateway",
    "LegacyP115OrganizeMutationGateway",
    "LegacyP115PlanNamingGateway",
    "LegacyP115RenamePreviewGateway",
    "LegacyP115RuleRecognitionGateway",
    "LegacyP115SortingRuleDirectoryGateway",
    "LegacyP115WashingPriorityConfigGateway",
    "P115ProviderError",
]
