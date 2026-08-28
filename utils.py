# utils.py (最终智能匹配版)

import re
from typing import Optional, Tuple, Any
from urllib.parse import quote_plus
import unicodedata
import logging

from etk.policy_defaults import (
    DEFAULT_AI_PROMPTS as DEFAULT_AI_PROMPTS,
    DEFAULT_COUNTRY_MAPPING,
    DEFAULT_JUNK_FILE_PATTERNS,
    DEFAULT_KEYWORD_MAPPING,
    DEFAULT_LANGUAGE_MAPPING,
    DEFAULT_RATING_MAPPING,
    DEFAULT_RATING_PRIORITY,
    DEFAULT_RELEASE_GROUP_MAPPING,
    DEFAULT_STUDIO_MAPPING,
    GENRE_TRANSLATION_PATCH,
    UNIFIED_RATING_CATEGORIES,
)
logger = logging.getLogger(__name__)
# 尝试导入 pypinyin，如果失败则创建一个模拟函数
try:
    from pypinyin import pinyin, Style
    PYPINYIN_AVAILABLE = True
except ImportError:
    PYPINYIN_AVAILABLE = False
    def pinyin(*args, **kwargs):
        # 如果库不存在，这个模拟函数将导致中文名无法转换为拼音进行匹配
        return []

def contains_chinese(text: Optional[str]) -> bool:
    """检查字符串是否包含中文字符。"""
    if not text:
        return False
    for char in text:
        if '\u4e00' <= char <= '\u9fff' or \
           '\u3400' <= char <= '\u4dbf' or \
           '\uf900' <= char <= '\ufaff':
            return True
    return False

_CHARACTER_ROLE_DISPLAY_PREFIX_RE = re.compile(r'^(?:(?:饰|配)[\s\u3000]+)+')


def strip_character_role_display_prefix(character_name: Optional[str]) -> str:
    """Remove ETK's optional display-only role prefix from a character name."""
    if not character_name:
        return ""
    return _CHARACTER_ROLE_DISPLAY_PREFIX_RE.sub('', str(character_name).strip()).strip()


def clean_character_name_static(character_name: Optional[str]) -> str:
    """
    统一格式化角色名：
    - 去除括号内容、前后缀如“饰、配、配音、as”
    - 中外对照时仅保留中文部分
    - 如果仅为“饰 Kevin”这种格式，清理前缀后保留英文，待后续翻译
    """
    if not character_name:
        return ""

    name = str(character_name).strip()

    # 移除括号和中括号的内容
    name = re.sub(r'\(.*?\)|\[.*?\]|（.*?）|【.*?】', '', name).strip()

    # 移除 as 前缀（如 "as Kevin"）
    name = re.sub(r'^(as\s+)', '', name, flags=re.IGNORECASE).strip()

    # 清理前缀中的“饰演/饰/配音/配”（不加判断，直接清理）
    prefix_pattern = r'^((?:饰演|饰|扮演|扮|配音|配|as\b)\s*)+'
    name = re.sub(prefix_pattern, '', name, flags=re.IGNORECASE).strip()

    # 清理后缀中的“饰演/饰/配音/配”
    suffix_pattern = r'(\s*(?:饰演|饰|配音|配))+$'
    name = re.sub(suffix_pattern, '', name).strip()

    # 处理中外对照：“中文+英文” 或 “英文+中文” 形式，只保留中文部分
    if re.search(r'[\u4e00-\u9fa5]', name) and re.search(r'[a-zA-Z]', name):
        # 1. 优先尝试按常见分隔符 (/, |) 拆分 (例如 "ShenWang/王忱")
        if '/' in name or '|' in name:
            parts = re.split(r'[/|]', name)
            for part in parts:
                # 找到包含中文的那一部分
                if re.search(r'[\u4e00-\u9fa5]', part):
                    # 提取出中文部分后，剔除可能残留的英文字母，并清理首尾空格
                    return re.sub(r'[a-zA-Z]', '', part).strip()
        
        # 2. 如果没有明显分隔符 (例如 "ShenWang王忱" 或 "王忱 ShenWang")
        # 直接暴力剔除所有英文字母，并压缩多余的空格
        clean_name = re.sub(r'[a-zA-Z]', '', name)
        return re.sub(r'\s+', ' ', clean_name).strip()

    # 如果只有外文，或清理后是英文，保留原值，等待后续翻译流程
    return name.strip()

def generate_search_url(provider: str, title: str, year: Optional[int] = None) -> str:
    """
    【V5 - 语法修复最终版】
    - 修复了 UnboundLocalError，确保变量在使用前被正确定义。
    - 统一了搜索词的生成逻辑，使其更加健壮。
    """
    if not title:
        return ""
    
    # 1. 统一准备搜索词和编码，确保变量在所有分支中都可用
    # 对于网页搜索，带上年份有助于消除歧义
    search_term = f"{title} {year}" if year else title
    encoded_term = quote_plus(search_term)
    
    # 2. 现在，可以安全地根据 provider 选择返回不同的 URL 格式
    if provider == 'baike':
        # 使用百度网页搜索
        return f"https://www.baidu.com/s?wd={encoded_term}"
    
    elif provider == 'wikipedia':
        # 使用 Google 站内搜索维基百科
        return f"https://www.google.com/search?q={encoded_term}+site%3Azh.wikipedia.org"
        
    else:
        # 默认回退到 Google 网页搜索
        return f"https://www.google.com/search?q={encoded_term}"

# --- ★★★ 全新的智能名字匹配核心逻辑 ★★★ ---
def normalize_name_for_matching(name: Optional[str]) -> str:
    """
    将名字极度标准化，用于模糊比较。
    转小写、移除所有非字母数字字符、处理 Unicode 兼容性。
    例如 "Chloë Grace Moretz" -> "chloegracemoretz"
    """
    if not name:
        return ""
    # NFKD 分解可以将 'ë' 分解为 'e' 和 '̈'
    nfkd_form = unicodedata.normalize('NFKD', str(name))
    # 只保留基本字符，去除重音等组合标记
    ascii_name = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # 转小写并只保留字母和数字
    return ''.join(filter(str.isalnum, ascii_name.lower()))

# --- 音视频流/字幕流特色标签映射 ---
# 用于识别并标准化 DYSY、CCTV、上译、公映 等特色标签
DEFAULT_STREAM_FEATURE_MAPPING = [
    {
        "label": "上译",  # 统一标准化为“上译”
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])SY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])DYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CH-DYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GP-DYSY(?![A-Za-z0-9])",
            r"上译",
            r"东影上译",
            r"泰盛上译",
            r"上海电影译制",
            r"上海电影配音",
            r"上海译制",
            r"上海配音",
            r"公映上译",
            r"上譯"
        ],
    },
    {
        "label": "公映",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])GY(?![A-Za-z0-9])",
            r"公映",
            r"院线配音",
            r"影院版"
        ],
    },
    {
        "label": "长译",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])CY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYCY(?![A-Za-z0-9])",
            r"长译",
            r"长春电影"
        ],
    },
    {
        "label": "京译",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])JY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYJY(?![A-Za-z0-9])",
            r"京译",
            r"中影配音",
            r"北京电影"
        ],
    },
    {
        "label": "八一",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"八一",  
        ],
    },
    {
        "label": "六区",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"六区",  
        ],
    },
    {
        "label": "华纳",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"华纳",  
        ],
    },
    {
        "label": "中影",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"中影",  
        ],
    },
    {
        "label": "央视",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])CCTV(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GP-CCTV(?![A-Za-z0-9])",
            r"央视",
            r"CCTV6"
        ],
    },
    {
        "label": "台配",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"台配",
            r"台湾配音"
        ],
    },
    {
    "label": "台湾",
    "types": ["Subtitle"],
    "patterns": [
        "(?<![A-Za-z0-9])TW(?![A-Za-z0-9])",
        "台灣",
        "臺灣",
        "台湾",
        "台配",
        "台灣配音",
        "臺灣配音",
        "台湾配音"
    ]
    },
    {
    "label": "香港",
    "types": ["Subtitle"],
    "patterns": [
        "(?<![A-Za-z0-9])HK(?![A-Za-z0-9])",
        "港配",
        "香港配音",
        "港",
    ]
    },
    {
        "label": "国语",
        "types": ["Subtitle"],
        "patterns": [
            r"国语",
            r"国配",
        ],
    },
    {
        "label": "官译",
        "types": ["Subtitle"],
        "patterns": [
            r"官译",
            r"官方",
        ],
    },
    {
        "label": "原声",
        "types": ["Subtitle"],
        "patterns": [
            r"原声",
            r"原音",
        ],
    },
    {
        "label": "特效",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])TX(?![A-Za-z0-9])",
            r"特效",
        ],
    },
    {
        "label": "HDR",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])HDR(?![A-Za-z0-9])",
            r"HDR",
        ],
    },
    {
        "label": "SDR",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])SDR(?![A-Za-z0-9])",
            r"SDR",
        ],
    },
    {
        "label": "DoVi",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])DoVi(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])DV(?![A-Za-z0-9])",
            r"DoVi",
        ],
    },
    {
        "label": "拉美",
        "types": ["Subtitle", "Audio"],
        "patterns": [
            r"\bLatin\s*America\b",
            r"\bLatin\s*American\b",
            r"\bLatin[-_\s]*America\b",
            r"\bLatin[-_\s]*American\b",
            r"\bLATAM\b",
            r"\bLatinoamerica\b",
            r"\bLatinoam[eé]rica\b",
            r"拉美",
            r"拉丁美洲",
        ],
    },
    {
        "label": "巴西",
        "types": ["Subtitle", "Audio"],
        "patterns": [
            r"\bBrazil\b",
            r"\bBrasil\b",
            r"\bBrazilian\b",
            r"\bBrasilian\b",
            r"\bBrasileiro\b",
            r"\bBrasileira\b",
            r"\bPortuguese\s*\(?Brazil\)?\b",
            r"\bPortuguese\s*[-_]\s*Brazil\b",
            r"巴西",
        ],
    },
    {
        "label": "听障",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])SDH(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CC(?![A-Za-z0-9])",
            r"hearing impaired",
            r"hard of hearing",
            r"听障",
        ],
    },
    {
        "label": "导评",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"Director'?s Commentary",
            r"Audio Commentary",
            r"Commentary",
            r"导评",
        ],
    },
]

# --- 音轨/字幕无意义压制组/字幕组过滤名单 ---
# 只要出现在这里的词，都会从音轨和字幕的标题中被无情抹除
STREAM_TITLE_GARBAGE_FILTER = [
    "麦哈", "说一不二", "人人字幕组", "人人影视", "远鉴字幕组", "衣柜字幕组", 
    "霸王龙压制组", "字幕组", "压制组", "手抄", "调轴", "精校", "原创", "校对", 
    "后期", "翻译", "制作", "发布", "团队", "组", "字幕", "配音", "合金弹头",
    "山茶树", "木木"
]

def clean_stream_garbage_words(text: str) -> str:
    """
    清理音轨/字幕标题中的无意义压制组、字幕组等干扰词汇。
    """
    if not text:
        return ""
    for garbage in STREAM_TITLE_GARBAGE_FILTER:
        text = text.replace(garbage, "")
    return text.strip()


# --- 分级计算通用逻辑 (含 Adult 强匹配) ---
def get_rating_label(details: dict, media_type: str, rating_map: Optional[dict] = None, priority: Optional[list] = None) -> str:
    """
    根据 TMDb 详情、媒体类型和配置，计算统一的分级标签 (Label)。
    
    逻辑：
    1. 【Adult 强匹配】如果 TMDb 标记为 adult=True，且配置中有 emby_value=15 的项，直接返回该标签。
    2. 【优先级遍历】按照 priority 配置的国家顺序查找分级。
    3. 【映射转换】将找到的国家分级代码转换为统一的中文 Label。
    """
    if rating_map is None: rating_map = DEFAULT_RATING_MAPPING
    if priority is None: priority = DEFAULT_RATING_PRIORITY

    # 1. ★★★ Adult 强匹配 (最高优先级) ★★★
    # 如果 TMDb 明确标记为成人内容
    if details.get('adult') is True:
        # 遍历所有国家的配置，寻找任意一个定义了 emby_value=15 (成人) 的标签
        # 通常在 US 里配置了 XXX -> 成人 -> 15
        for country_rules in rating_map.values():
            for rule in country_rules:
                if rule.get('emby_value') == 15:
                    return rule['label']

    # 2. 准备源数据
    rating_code = None
    rating_country = None
    
    # 获取原产国 (用于处理 'ORIGIN' 优先级)
    origin_countries = details.get('origin_country', [])
    if not origin_countries and 'production_countries' in details:
        origin_countries = [c.get('iso_3166_1') for c in details['production_countries']]
    
    # 3. 遍历优先级
    for country in priority:
        target_countries = []
        if country == 'ORIGIN':
            target_countries = origin_countries
        else:
            target_countries = [country]
        
        if not target_countries: continue

        for target_c in target_countries:
            found_code = None
            
            if media_type == 'tv':
                # TV 逻辑: content_ratings.results
                results = details.get('content_ratings', {}).get('results', [])
                found = next((r for r in results if r['iso_3166_1'] == target_c), None)
                if found: found_code = found.get('rating')
            else:
                # Movie 逻辑: release_dates.results
                results = details.get('release_dates', {}).get('results', [])
                country_data = next((r for r in results if r['iso_3166_1'] == target_c), None)
                if country_data:
                    # 电影可能有多个分级 (不同版本)，优先取第一个非空的 certification
                    for rel in country_data.get('release_dates', []):
                        if rel.get('certification'):
                            found_code = rel.get('certification')
                            break
            
            if found_code:
                rating_code = found_code
                rating_country = target_c
                break
        
        if rating_code: break

    # 4. 映射到 Label
    if rating_code and rating_country:
        # 查找对应的 Label
        country_rules = rating_map.get(rating_country, [])
        
        # 尝试完全匹配
        for rule in country_rules:
            if rule['code'] == rating_code:
                return rule['label']
        
        # 如果没找到完全匹配，尝试不区分大小写
        for rule in country_rules:
            if rule['code'].lower() == rating_code.lower():
                return rule['label']

    return '未知'

# --- ★★★ 万能 STRM 提取器 (支持自定义正则) ★★★ ---
def extract_pickcode_from_strm_url(url: str) -> Optional[str]:
    """只从 ETK 官方 STRM 播放地址中提取 115 PC 码。"""
    if not url or not isinstance(url, str):
        return None
        
    if '/p115/play/' in url:
        return url.split('/p115/play/')[-1].split('/')[0].split('?')[0].strip()

    return None

def get_pinyin_initials(text: str) -> str:
    """获取中文拼音首字母大写，用于 Emby 的 sorttitle"""
    if not text:
        return ""
    if not PYPINYIN_AVAILABLE:
        return text
    try:
        initials = pinyin(text, style=Style.FIRST_LETTER, strict=False)
        result = ""
        for i in initials:
            char = i[0]
            if char.isalnum():
                result += char.upper()
        return result if result else text
    except Exception:
        return text
    
def clean_invisible_chars(text: str) -> str:
    """
    终极字符串清洗：去除所有不可见的零宽字符、特殊排版符号，并将特殊空格转换为普通空格。
    """
    if not text:
        return ""
    
    # 1. 替换特殊空格为普通空格
    # \xa0: 不换行空格 (NBSP)
    # \u3000: 全角空格 (中文输入法下的空格)
    text = text.replace('\xa0', ' ').replace('\u3000', ' ')
    
    # 2. 使用正则剔除所有“零宽字符”和“排版控制字符”
    # \u200b-\u200f: 零宽空格、零宽连字、从左至右标记等
    # \u202a-\u202e: 文本方向控制符
    # \u2060-\u206f: 词语连接符等不可见数学符号
    # \ufeff: 字节顺序标记 (BOM)
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
    
    # 3. 将连续的多个空格压缩为一个，并去除首尾空格
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

# --- TG 频道监听默认正则预设 ---
DEFAULT_TG_REGEX = {
    "tmdb": [
        r'TMDB(?:\s*ID)?[:：\s]*(\d+)'
    ],
    "title_year": [
        r'(?:电视剧 | 电影 | 名称)[:：\s]*([^\n]+?)\s*\((\d{4})\)',
        r'^([^\n]+?)\s*\((\d{4})\)'
    ],
    "password_url": [
        r'(?:password|pwd)=([a-zA-Z0-9]{4})'
    ],
    "password_text": [
        r'(?:password=|访问码 | 提取码 | 密码)[:：=\s]*([a-zA-Z0-9]{4})'
    ]
}

def clean_non_chinese_chars(text: Optional[str]) -> str:
    """
    清理字符串，只保留中文字符和中文括号。
    
    移除所有非中文字符，包括：
    - 英文字母、数字
    - 标点符号（除中文括号外）
    - 特殊符号、表情符号
    - 空白字符
    - 其他非汉字字符
    
    保留的字符范围：
    - 基本汉字：\u4e00-\u9fff
    - 扩展 A 区：\u3400-\u4dbf
    - 扩展 B-F 区：\u20000-\u2a6df, \u2a700-\u2b73f, \u2b740-\u2b81f, \u2b820-\u2ceaf, \u2ceb0-\u2ebef
    - 兼容汉字：\uf900-\ufaff
    - 中文括号：（）
    
    Args:
        text: 输入字符串
        
    Returns:
        只包含中文字符和中文括号的字符串
    """
    if not text:
        return ""
    
    result = []
    for char in str(text):
        code_point = ord(char)
        # 基本汉字
        if 0x4e00 <= code_point <= 0x9fff:
            result.append(char)
        # 扩展 A 区
        elif 0x3400 <= code_point <= 0x4dbf:
            result.append(char)
        # 兼容汉字
        elif 0xf900 <= code_point <= 0xfaff:
            result.append(char)
        # 扩展 B-F 区（代理对）
        elif 0x20000 <= code_point <= 0x2ebef:
            result.append(char)
        # 中文括号
        elif char in '（）':
            result.append(char)
    
    return ''.join(result)
