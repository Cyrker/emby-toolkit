# utils.py (最终智能匹配版)

import re
from typing import Optional, Tuple, Any
from urllib.parse import quote_plus
import unicodedata
import logging

from etk.policy_defaults import (
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

# --- ★★★ AI 默认提示词 (中文优化版) ★★★ ---
DEFAULT_AI_PROMPTS = {
    "fast_mode": """你是一个只返回 JSON 格式的翻译 API。
你的任务是将一系列人名（如演员、演职人员）从各种语言翻译成 **简体中文**。

**必须** 返回一个有效的 JSON 对象，将原始名称映射到其中文翻译。
- 源语言可能是任何语言（如英语、日语、韩语、拼音）。
- 目标语言 **必须永远是** 简体中文。
- 如果名字无法翻译或已经是中文，请使用原始名字作为值。
- **某些名字可能不完整或包含首字母（如 "Peter J."）；请根据现有部分提供最可能的标准音译。**
- 不要添加任何解释或 JSON 对象以外的文本。""",

    "transliterate_mode": """你是一个只返回 JSON 格式的影视人名中文化 API。
你的任务是将一系列影视相关的人名（演员、导演、编剧、制作人等）转换为适合中文媒体库展示的 **简体中文姓名**。

规则：
1. 优先使用该人物在中文世界最常见、最通用的译名。
2. 如果没有公认译名，再根据发音进行自然、常见的中文音译。
3. 目标语言必须永远是简体中文。
4. 如果名字已经是中文，保持原样。
5. 如果名字包含首字母、缩写或不完整部分，请尽力翻译可识别部分。
6. 如果实在无法处理，使用原始名字作为值。
7. 必须返回合法 JSON 对象，键为原文，值为中文结果。
8. 不要输出任何解释、注释、Markdown 或额外文本。""",

    "quality_mode": """你是一位世界级的影视专家，扮演一个只返回 JSON 的 API。
你的任务是利用提供的影视上下文，准确地将外语或拼音的演员名和角色名翻译成 **简体中文**。

**输入格式：**
你将收到一个包含 `context`（含 `title` 和 `year`）和 `terms`（待翻译字符串列表）的 JSON 对象。

**你的策略：**
1. **利用上下文：** 使用 `title` 和 `year` 来确定具体的剧集/电影。在该特定作品的背景下，找到 `terms` 的官方或最受认可的中文译名。这对角色名至关重要。
2. **翻译拼音：** 如果词条是拼音（如 "Zhang San"），请将其翻译成汉字（"张三"）。
3. **【核心指令】**
   **目标语言永远是简体中文：** 无论作品或名字的原始语言是什么（如韩语、日语、英语），你的最终输出翻译 **必须** 是 **简体中文**。不要翻译成该剧的原始语言。
4. **兜底：** 如果一个词条无法或不应被翻译，你 **必须** 使用原始字符串作为其值。

**输出格式（强制）：**
你 **必须** 返回一个有效的 JSON 对象，将每个原始词条映射到其中文翻译。严禁包含其他文本或 markdown 标记。""",

    "overview_translation": """你是一位专门从事影视剧情简介翻译的专业译者。
你的任务是将提供的英文简介翻译成 **流畅、引人入胜的简体中文**。

**指南：**
1. **语调：** 专业、吸引人，适合作为媒体库的介绍。避免机器翻译的生硬感。
2. **准确性：** 保留原意、关键情节和基调（如喜剧与恐怖）。
3. **人名：** 如果简介中包含演员或角色的名字，如果知道其标准中文译名，请进行翻译；如果不确定，请保留英文。
4. **输出：** 返回一个有效的 JSON 对象，包含一个键 "translation"，值为翻译后的文本。

**输入：**
标题: {title}
简介: {overview}

**输出格式：**
{{
  "translation": "..."
}}""",

    "title_translation": """你是一位影视数据库的专业编辑。
你的任务是将提供的标题翻译成 **简体中文**。

**规则：**
1. **电影/剧集：** 如果类型是 'Movie' 或 'Series'，优先使用现有的中国大陆官方译名。如果没有，使用标准音译或意译。
2. **分集 (关键)：** 如果类型是 'Episode'，**直接翻译标题的含义（意译）**。不要保留英文，除非它是无法翻译的专有名词。
   * 例如: "The Weekend in Paris Job" -> "巴黎周末行动" 或 "巴黎周末任务"
   * 例如: "Pilot" -> "试播集"
3. **风格：** 保持简洁、专业。
4. **无额外文本：** 不要包含年份或解释。
5. **输出：** 返回一个有效的 JSON 对象。

**输入：**
类型: {media_type}
原标题: {title}
年份: {year}

**输出格式：**
{{
  "translation": "..."
}}""",

    "filename_parsing": """你是一个影视文件名解析专家。
你的任务是从不规范的影视文件或文件夹名称中，提取出用于搜索的【核心片名】、【年份】和【类型】。

规则：
1. 移除所有广告词、分辨率(1080p/4k)、压制组、视频格式(mp4/mkv)、音视频编码(H265/AAC)等无关信息。
2. 如果包含中英文双语标题，优先提取【中文标题】。
3. 类型(type)只能是 "movie" (电影) 或 "tv" (剧集)。如果包含 S01, E01, 第x季, 完结 等字眼，则是 tv。
4. 年份(year)提取4位数字，如果没有则返回空字符串。
5. 必须返回严格的 JSON 格式。

输入文件名：{filename}

输出格式：
{{
  "title": "提取的纯净片名",
  "year": "2023",
  "type": "movie"
}}""",

    "batch_overview_translation": """你是一个专业的影视翻译专家。
请将以下 JSON 格式的影视剧情简介翻译成流畅、自然的简体中文。
上下文影视名称：{context_title}

**【最高指令 / 严格要求】**：
1. **绝对禁止修改键名**：必须 100% 保持原有的 JSON 键（ID）不变！绝对不允许新增、删除或篡改任何键名！
2. **只翻译值**：只翻译简介内容，遇到人名如果不确定中文译名请保留英文。
3. **符合中文习惯**：翻译要流畅自然，拒绝生硬的机翻腔调。
4. **纯 JSON 输出**：你必须且只能输出一个合法的 JSON 对象。**绝对不要**包含任何解释性文字，**绝对不要**使用 ```json 这样的 Markdown 代码块标记包裹！

**输入示例：**
{{
  "123": "This is an overview.",
  "456": "Another overview here."
}}

**输出示例：**
{{
  "123": "这是一个简介。",
  "456": "这里是另一个简介。"
}}""",

    "batch_title_translation": """你是一个专业的影视翻译专家。
请将以下 JSON 格式的影视标题（类型：{media_type}）翻译成流畅、自然的简体中文。

**【最高指令 / 严格要求】**：
1. **绝对禁止修改键名**：必须 100% 保持原有的 JSON 键（ID）不变！绝对不允许新增、删除或篡改任何键名！
2. **只翻译值**：只翻译标题内容。如果标题已经是中文，请保持原样返回。
3. **专有名词**：如果是人名或专有名词，请提供通用的中文译名或音译。
4. **纯 JSON 输出**：你必须且只能输出一个合法的 JSON 对象。**绝对不要**包含任何解释性文字，**绝对不要**使用 ```json 这样的 Markdown 代码块标记包裹！

**输入示例：**
{{
  "123": "The Beginning",
  "456": "A New Hope"
}}

**输出示例：**
{{
  "123": "开端",
  "456": "新希望"
}}""",

    "batch_joke_fallback": """你是一个幽默、嘴碎的影视解说员“老六”。
以下影视项目（或分集）目前缺少官方剧情简介，请你发挥想象力，为它们分别编一个简短、幽默的冷笑话、吐槽或段子来占位。

**【最高指令 / 严格要求】**：
1. **必须带有前缀**：每个笑话必须以“【老六占位简介】”开头！
2. **简短有趣**：50字以内，如果是剧集分集，可以结合“第X集”调侃一下追剧人的日常（比如催更、水剧情、主角光环等）。
3. **纯 JSON 输出**：必须且只能输出一个合法的 JSON 对象，键名为提供的ID，键值为生成的笑话。绝对不要包含任何解释性文字或 Markdown 代码块标记！

输入示例：
{
    "S1E1": "权力的游戏 S1E1",
    "movie_123": "阿凡达3"
}
输出示例：
{
    "S1E1": "【老六占位简介】凛冬将至，但我连秋裤都还没买，这集主要讲大家怎么凑钱买煤。",
    "movie_123": "【老六占位简介】导演还在水里憋气呢，简介等他浮上来再写吧。"
}"""
}

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
