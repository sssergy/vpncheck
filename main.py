import os
import re
import socket
import ssl
import time
import json
import requests
import base64
import websocket
import shutil
import threading
import sqlite3
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from contextlib import closing

# ------------------ Настройки ------------------
VERBOSE = False  # Отключаем вывод по каждому ключу, будет прогресс-бар

BASE_DIR = "checked"
FOLDER_RU = os.path.join(BASE_DIR, "RU_Best")
FOLDER_EURO = os.path.join(BASE_DIR, "My_Euro")

if os.path.exists(FOLDER_RU):
    shutil.rmtree(FOLDER_RU)
if os.path.exists(FOLDER_EURO):
    shutil.rmtree(FOLDER_EURO)
os.makedirs(FOLDER_RU, exist_ok=True)
os.makedirs(FOLDER_EURO, exist_ok=True)

TIMEOUT = 5
socket.setdefaulttimeout(TIMEOUT)
THREADS = 40

CACHE_HOURS = 6
CHUNK_LIMIT = 1000
EURO_CHUNK_LIMIT = 500
MAX_KEYS_TO_CHECK = 30000

MAX_PING_MS = 3000
FAST_LIMIT = 3000
MAX_HISTORY_AGE = 2 * 24 * 3600

# Дисковый кэш IP → страна
IP_CACHE_FILE = os.path.join(BASE_DIR, "ip_cache.json")
IP_CACHE_MAX_AGE_DAYS = 30

# Чёрный список SQLite
BLACKLIST_DB = os.path.join(BASE_DIR, "blacklist.db")
BLACKLIST_DAYS = 7   # блокировка на 7 дней

# ip-api: не более ~40 req/min — берём 38 для запаса
GEO_API_RATE_LIMIT = 38
GEO_API_WINDOW = 60.0

RU_FILES = ["ru_white_part1.txt", "ru_white_part2.txt", "ru_white_part3.txt", "ru_white_part4.txt"]
EURO_FILES = ["my_euro_part1.txt", "my_euro_part2.txt", "my_euro_part3.txt"]

HISTORY_FILE = os.path.join(BASE_DIR, "history.json")
MY_CHANNEL = "@vlesstrojan"

# ------------------ ОБНОВЛЁННЫЕ ИСТОЧНИКИ (без дубликатов) ------------------
URLS_RU = [
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/BLACK_VLESS_RUS_mobile.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/BLACK_SS%2BAll_RUS.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/WHITE-CIDR-RU-all.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/WHITE-CIDR-RU-checked.txt",
    "https://github.com/igareck/vpn-configs-for-russia/blob/main/WHITE-SNI-RU-all.txt",
    "https://raw.githubusercontent.com/zieng2/wl/main/vless.txt",
    "https://raw.githubusercontent.com/LowiKLive/BypassWhitelistRu/refs/heads/main/WhiteList-Bypass_Ru.txt",
    "https://raw.githubusercontent.com/zieng2/wl/main/vless_universal.txt",
    "https://raw.githubusercontent.com/vsevjik/OBSpiskov/refs/heads/main/wwh",
    "https://jsnegsukavsos.hb.ru-msk.vkcloud-storage.ru/love",
    "https://etoneya.a9fm.site/1",
    "https://s3c3.001.gpucloud.ru/vahe4xkwi/cjdr",
    # НОВЫЕ источники (из списка, уникальные)
    "https://raw.githubusercontent.com/Argh73/VpnConfigCollector/refs/heads/main/Splitted-By-Country/Russia.txt",
    "https://raw.githubusercontent.com/Omid-0x0x0x/vless/main/configs/vless_config_73.txt",
    "https://raw.githubusercontent.com/WhitePrime/xraycheck/main/configs/white-list_available(top100)",
    "https://raw.githubusercontent.com/mohamadfg-dev/telegram-v2ray-configs-collector/refs/heads/main/category/vless.txt",
    "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/refs/heads/main/working/countries/Russia.txt",
    "https://raw.githubusercontent.com/vpnineh/config/main/sub/mix_protocol/mix_vless_5.txt",
    "https://github.com/Urbanica/vpn-sub/raw/main/sub.txt",
    "https://raw.githubusercontent.com/Omid-0x0x0x/vless/main/configs/vless_config_80.txt",
    "https://github.com/LimeHi/LimeVPN/raw/main/LimeVPN.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/refs/heads/main/v2ray_configs/seperated_by_protocol/vmess.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/refs/heads/main/v2ray_configs/seperated_by_protocol/other.txt",
    "https://raw.githubusercontent.com/Delta-Kronecker/V2ray-Config/refs/heads/main/config/protocols/trojan.txt",
    "https://raw.githubusercontent.com/Delta-Kronecker/V2ray-Config/refs/heads/main/config/protocols/vmess.txt",
    "https://github.com/WhitePrime/xraycheck/raw/main/configs/white-list_available_st",
    "https://raw.githubusercontent.com/OZRED/vless/refs/heads/main/BezRF",
    "https://raw.githubusercontent.com/WhitePrime/xraycheck/main/configs/white-list_available_st",
    "https://raw.githubusercontent.com/nscl5/5/main/configs/at/all.txt",
    "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/main/mini/m1n1-5ub-6.txt",
    "https://raw.githubusercontent.com/vpnineh/config/main/sub/mix_protocol/mix_vless_1.txt",
    "https://raw.githubusercontent.com/vpnineh/config/main/sub/mix_protocol/mix_vless_2.txt",
    "https://raw.githubusercontent.com/vpnineh/config/main/sub/mix_protocol/mix_vless_3.txt",
    "https://github.com/Argh94/Proxy-List/raw/refs/heads/main/All_Config.txt",
    "https://github.com/KiryaScript/white-lists/raw/refs/heads/main/githubmirror/20.txt",
    "https://github.com/WhitePrime/xraycheck/raw/main/configs/white-list_available",
    "https://raw.githubusercontent.com/55prosek-lgtm/vpn_config_for_russia/refs/heads/main/whitelist.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/main/v2ray_configs/mixed/subscription-19.txt",
    "https://raw.githubusercontent.com/Ganjabady/XC/refs/heads/main/subscriptions/regions/RU.txt",
    "https://raw.githubusercontent.com/WhitePrime/xraycheck/main/configs/white-list_available",
    "https://github.com/KiryaScript/white-lists/raw/refs/heads/main/githubmirror/26.txt",
    "https://raw.githubusercontent.com/Ai123999/WhiteKeys/main/WhiteKeys",
    "https://raw.githubusercontent.com/FLEXIY0/matryoshka-vpn/main/configs/russia_whitelist.txt",
    "https://raw.githubusercontent.com/liMilCo/v2r/refs/heads/main/all_configs.txt",
    "https://raw.githubusercontent.com/terik21/HiddifySubs-VlessKeys/main/WhiteKeys",
    "https://raw.githubusercontent.com/vpnineh/config/main/sub/mix_protocol/mix_vless_4.txt",
    "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/refs/heads/main/splitted-by-country/Russia.txt",
    "https://gbr.mydan.online/configs",
    "https://github.com/hardcrabe/vpncrab/raw/main/nodes.txt",
    "https://raw.githubusercontent.com/kort0881/proxy-auto-checker/main/results/premium/elite.txt",
    "https://github.com/KiryaScript/white-lists/raw/refs/heads/main/githubmirror/27.txt",
    "https://raw.githubusercontent.com/Danialsamadi/v2go/main/AllConfigsSub.txt",
    "https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/main/configs/ru/vless.txt",
    "https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/refs/heads/main/configs/ru/all.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/refs/heads/main/Countries/Russia.txt",
    "https://raw.githubusercontent.com/nscl5/5/main/configs/all.txt",
    "https://github.com/Mr-Meshky/vify/raw/main/configs/all.txt",
    "https://raw.githubusercontent.com/Argh94/V2RayAutoConfig/refs/heads/main/configs/Vless.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/refs/heads/main/all_configs.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/new/by_protocol/vless/vless_003.txt",
    "https://github.com/4n0nymou3/multi-proxy-config-fetcher/raw/main/configs/proxy_configs.txt",
    "https://github.com/4n0nymou3/multi-proxy-config-fetcher/raw/refs/heads/main/configs/proxy_configs_tested.txt",
    "https://github.com/ShatakVPN/ConfigForge-V2Ray/raw/main/configs/all.txt",
    "https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/main/configs/proxy_configs.txt",
    "https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/main/configs/proxy_configs_tested.txt",
    "https://raw.githubusercontent.com/hamedcode/port-based-v2ray-configs/main/sub/vless.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/main/githubmirror/26.txt",
    "https://raw.githubusercontent.com/vpnineh/config/refs/heads/main/sub/countries/RU.txt",
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/vless.txt",
    "https://raw.githubusercontent.com/whoahaow/rjsxrd/main/githubmirror/bypass/bypass-all.txt",
    "https://raw.githubusercontent.com/whoahaow/rjsxrd/refs/heads/main/githubmirror/bypass/bypass-all.txt",
    "https://github.com/sakha1370/OpenRay/raw/main/output/all_valid_proxies.txt",
    "https://github.com/sakha1370/OpenRay/raw/refs/heads/main/output/all_valid_proxies.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/main/githubmirror/1.txt",
    "https://raw.githubusercontent.com/sakha1370/OpenRay/main/output/all_valid_proxies.txt",
    "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/main/mini/m1n1-5ub-5.txt",
    "https://github.com/sevcator/5ubscrpt10n/raw/main/protocols/vl.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/main/githubmirror/2.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/main/v2ray_configs/seperated_by_protocol/vless.txt",
    "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/main/protocols/vl.txt",
    "https://raw.githubusercontent.com/Delta-Kronecker/V2ray-Config/main/config/all_configs.txt",
    "https://github.com/oaoa4676-alt/vpn-klysh/raw/main/white-list",
    "https://cdn.jsdelivr.net/gh/EtoNeYaProject/EtoNeYaProject.github.io@refs/heads/main/1",
    "https://raw.githubusercontent.com/OZRED/vless/refs/heads/main/ozred_bot",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/main/whitelist",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/main/1",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/main/2",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/main/vless",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/refs/heads/main/1",
    "https://raw.githubusercontent.com/EtoNeYaProject/etoneyaproject.github.io/refs/heads/main/2",
    "https://github.com/AirLinkVPN/AirLinkVPN.github.io/raw/main/1.txt",
    "https://github.com/EtoNeYaProject/etoneyaproject.github.io/raw/main/test",
    "https://raw.githubusercontent.com/AirLinkVPN/AirLinkVPN.github.io/main/1.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/WHITE-CIDR-RU-checked.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/WHITE-CIDR-RU-all.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/WHITE-CIDR-RU-all.txt",
    "https://github.com/zieng2/wl/raw/main/vless_universal.txt",
    "https://raw.githubusercontent.com/zieng2/wl/main/vless_lite.txt",
    "https://raw.githubusercontent.com/zieng2/wl/main/vless_universal.txt",
    "https://raw.githubusercontent.com/zieng2/wl/refs/heads/main/vless_universal.txt",
    "https://raw.githubusercontent.com/RKPchannel/RKP_bypass_configs/main/configs/url_work.txt",
    "https://github.com/ginolrewadsb11/studious-umbrella/raw/main/bobi_vpn.txt",
    "https://github.com/seknei3/psychic-fiestas/raw/main/bobi_vpn.txt",
    "https://github.com/seknei3/psychic-fiestas/raw/main/vpn_renamed.txt",
    "https://raw.githubusercontent.com/OZRED/vless/refs/heads/main/SuicideEtoExit",
]

URLS_MY = [
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/new/all_new.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/vmess.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/trojan.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/ss.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/hysteria.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/hysteria2.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/hy2.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/refs/heads/main/githubmirror/clean/tuic.txt",
]

EURO_CODES = {
    "NL", "DE", "FI", "GB", "FR", "SE", "PL", "CZ", "AT", "CH",
    "IT", "ES", "NO", "DK", "BE", "IE", "LU", "EE", "LV", "LT"
}
BAD_MARKERS = ["CN", "IR", "KR", "BR", "IN", "RELAY", "POOL", "🇨🇳", "🇮🇷", "🇰🇷"]

RU_MARKERS_STRICT = [
    ".ru", "moscow", "msk", "spb", "saint-peter", "russia",
    "россия", "москва", "питер", "ru-", "-ru.",
    "178.154.", "77.88.", "5.255.", "87.250.",
    "95.108.", "213.180.", "195.208.",
    "91.108.", "149.154.",
]

# ------------------ Страна → название + флаг ------------------

COUNTRY_NAMES_RU = {
    "RU": "Россия", "NL": "Нидерланды", "DE": "Германия", "FI": "Финляндия",
    "GB": "Великобритания", "FR": "Франция", "SE": "Швеция", "PL": "Польша",
    "CZ": "Чехия", "AT": "Австрия", "CH": "Швейцария", "IT": "Италия",
    "ES": "Испания", "NO": "Норвегия", "DK": "Дания", "BE": "Бельгия",
    "IE": "Ирландия", "LU": "Люксембург", "EE": "Эстония", "LV": "Латвия",
    "LT": "Литва",
}

COUNTRY_FLAGS = {
    "RU": "🇷🇺", "NL": "🇳🇱", "DE": "🇩🇪", "FI": "🇫🇮", "GB": "🇬🇧",
    "FR": "🇫🇷", "SE": "🇸🇪", "PL": "🇵🇱", "CZ": "🇨🇿", "AT": "🇦🇹",
    "CH": "🇨🇭", "IT": "🇮🇹", "ES": "🇪🇸", "NO": "🇳🇴", "DK": "🇩🇰",
    "BE": "🇧🇪", "IE": "🇮🇪", "LU": "🇱🇺", "EE": "🇪🇪", "LV": "🇱🇻",
    "LT": "🇱🇹",
}

def country_to_title_ru(code: str) -> str:
    return COUNTRY_NAMES_RU.get(code, code or "UNKNOWN")

def country_to_flag(code: str) -> str:
    return COUNTRY_FLAGS.get(code, "")


# ==================== ЧЁРНЫЙ СПИСОК SQLITE ====================

_blacklist_conn = None

def get_blacklist_conn():
    global _blacklist_conn
    if _blacklist_conn is None:
        _blacklist_conn = sqlite3.connect(BLACKLIST_DB, check_same_thread=False)
        _blacklist_conn.execute("""
            CREATE TABLE IF NOT EXISTS blacklist (
                ip TEXT PRIMARY KEY,
                reason TEXT,
                block_time REAL
            )
        """)
        _blacklist_conn.commit()
    return _blacklist_conn

def is_ip_blacklisted(ip: str) -> bool:
    """Проверяет, заблокирован ли IP (если запись есть и срок не истёк)."""
    if not ip:
        return False
    conn = get_blacklist_conn()
    cutoff = time.time() - BLACKLIST_DAYS * 86400
    cur = conn.execute("SELECT 1 FROM blacklist WHERE ip = ? AND block_time > ?", (ip, cutoff))
    return cur.fetchone() is not None

def add_ip_to_blacklist(ip: str, reason: str):
    """Добавляет или обновляет IP в чёрном списке с текущим временем."""
    if not ip:
        return
    conn = get_blacklist_conn()
    conn.execute(
        "INSERT OR REPLACE INTO blacklist (ip, reason, block_time) VALUES (?, ?, ?)",
        (ip, reason, time.time())
    )
    conn.commit()

def remove_ip_from_blacklist(ip: str):
    """Удаляет IP из чёрного списка (при успешном соединении)."""
    if not ip:
        return
    conn = get_blacklist_conn()
    conn.execute("DELETE FROM blacklist WHERE ip = ?", (ip,))
    conn.commit()

def clean_old_blacklist():
    """Удаляет записи старше BLACKLIST_DAYS дней."""
    conn = get_blacklist_conn()
    cutoff = time.time() - BLACKLIST_DAYS * 86400
    conn.execute("DELETE FROM blacklist WHERE block_time < ?", (cutoff,))
    conn.commit()
    deleted = conn.total_changes
    if deleted:
        print(f"🧹 Очищено {deleted} устаревших записей из чёрного списка")


# ==================== GEO-API + КЭШИ ====================

_disk_ip_cache: dict = {}

def load_ip_cache():
    global _disk_ip_cache
    if os.path.exists(IP_CACHE_FILE):
        try:
            with open(IP_CACHE_FILE, "r", encoding="utf-8") as f:
                _disk_ip_cache = json.load(f)
        except Exception:
            _disk_ip_cache = {}
    cutoff = time.time() - IP_CACHE_MAX_AGE_DAYS * 86400
    _disk_ip_cache = {k: v for k, v in _disk_ip_cache.items() if v.get("time", 0) > cutoff}

def save_ip_cache():
    os.makedirs(BASE_DIR, exist_ok=True)
    try:
        with open(IP_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_disk_ip_cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

_ip_cache_lock = threading.Lock()
_host_to_ip: dict = {}
_host_ip_lock = threading.Lock()

def resolve_host(host: str) -> str | None:
    with _host_ip_lock:
        if host in _host_to_ip:
            return _host_to_ip[host]
    try:
        ip = socket.gethostbyname(host)
        with _host_ip_lock:
            _host_to_ip[host] = ip
        return ip
    except Exception:
        with _host_ip_lock:
            _host_to_ip[host] = None
        return None

_geo_rate_lock = threading.Lock()
_geo_request_times: list = []
_ip_api_disabled = False
_geo_stats = defaultdict(int)
_geo_stats_lock = threading.Lock()

def _inc_geo_stat(key: str):
    with _geo_stats_lock:
        _geo_stats[key] += 1

def _geo_api_wait_slot() -> bool:
    global _ip_api_disabled
    if _ip_api_disabled:
        return False
    with _geo_rate_lock:
        now = time.time()
        cutoff = now - GEO_API_WINDOW
        while _geo_request_times and _geo_request_times[0] < cutoff:
            _geo_request_times.pop(0)
        if len(_geo_request_times) >= GEO_API_RATE_LIMIT:
            sleep_time = GEO_API_WINDOW - (now - _geo_request_times[0]) + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
            now = time.time()
            cutoff = now - GEO_API_WINDOW
            while _geo_request_times and _geo_request_times[0] < cutoff:
                _geo_request_times.pop(0)
        _geo_request_times.append(time.time())
    return True


def detect_exit_country_via_http(proxy_host: str) -> str:
    global _ip_api_disabled
    ip = resolve_host(proxy_host)
    if not ip:
        return "UNKNOWN"
    with _ip_cache_lock:
        cached = _disk_ip_cache.get(ip)
    if cached:
        _inc_geo_stat("cache")
        return cached["country"]
    if _ip_api_disabled:
        return "UNKNOWN"
    if not _geo_api_wait_slot():
        return "UNKNOWN"
    try:
        r = requests.get(f"http://ip-api.com/json/{ip}?fields=countryCode", timeout=4)
        if r.status_code == 429:
            _ip_api_disabled = True
            print("⚠️  ip-api вернул 429 (rate limit) — geo-API отключён до конца запуска")
            return "UNKNOWN"
        if r.status_code == 200:
            code = r.json().get("countryCode", "UNKNOWN") or "UNKNOWN"
            with _ip_cache_lock:
                _disk_ip_cache[ip] = {"country": code, "time": time.time()}
            _inc_geo_stat("api")
            return code
    except Exception:
        pass
    return "UNKNOWN"


def get_country_fast(host: str, key_name: str) -> str:
    try:
        host_l = host.lower()
        name_u = key_name.upper()
        if host_l.endswith(".ru"):
            return "RU"
        if host_l.endswith(".de"):
            return "DE"
        if host_l.endswith(".nl"):
            return "NL"
        if host_l.endswith(".uk") or host_l.endswith(".co.uk"):
            return "GB"
        if host_l.endswith(".fr"):
            return "FR"
        for code in EURO_CODES:
            if code in name_u:
                return code
    except Exception:
        pass
    return "UNKNOWN"


def _has_many_ru_markers(host: str, key_str: str) -> bool:
    count = 0
    host_lower = host.lower()
    key_upper = key_str.upper()
    for marker in RU_MARKERS_STRICT:
        if marker.lower() in host_lower or marker.upper() in key_upper:
            count += 1
            if count >= 2:
                return True
    return False


def is_russian_exit(key_str: str, host: str, country: str) -> bool:
    if country == "RU":
        return True
    host_lower = host.lower()
    if host_lower.endswith(".ru"):
        return True
    for marker in RU_MARKERS_STRICT:
        if marker.lower() in host_lower:
            return True
    return False


def is_garbage_text(key_str: str) -> bool:
    upper = key_str.upper()
    for m in BAD_MARKERS:
        if m in upper:
            return True
    if ".ir" in key_str or ".cn" in key_str or "127.0.0.1" in key_str:
        return True
    return False


# ==================== Загрузка ключей ====================

def fetch_keys(urls, tag):
    out = []
    print(f"Загрузка {tag}...")
    for url in urls:
        try:
            if "github.com" in url and "/blob/" in url:
                url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                print(f"⚠️  Не удалось загрузить {url}: HTTP {r.status_code}")
                continue
            content = r.text.strip()
            if "://" not in content:
                try:
                    lines = base64.b64decode(content + "==").decode("utf-8", errors="ignore").splitlines()
                except Exception:
                    lines = content.splitlines()
            else:
                lines = content.splitlines()
            for l in lines:
                l = l.strip()
                if len(l) > 2000:
                    continue
                if l.startswith(("vless://", "vmess://", "trojan://", "ss://")):
                    if tag == "MY" and is_garbage_text(l):
                        continue
                    out.append((l, tag))
        except requests.exceptions.Timeout:
            print(f"⏰ Таймаут при загрузке {url}")
        except requests.exceptions.ConnectionError as e:
            print(f"🔌 Ошибка соединения с {url}: {e}")
        except Exception as e:
            print(f"❌ Ошибка загрузки {url}: {type(e).__name__} - {e}")
    return out


# ==================== Проверка одного ключа ====================

ERR_TIMEOUT = "timeout"
ERR_TLS = "tls"
ERR_DNS = "dns"
ERR_OTHER = "other"

_err_stats = defaultdict(int)
_err_stats_lock = threading.Lock()

def _inc_err(kind: str):
    with _err_stats_lock:
        _err_stats[kind] += 1


def check_single_key(data):
    key, tag = data
    try:
        if "@" not in key or ":" not in key:
            return None, None, None, None, key, ERR_OTHER
        part = key.split("@")[1].split("?")[0].split("#")[0]
        host_port = part.split(":")
        host = host_port[0]
        port = int(host_port[1])
    except Exception:
        return None, None, None, None, key, ERR_OTHER

    # Получаем IP для черного списка
    ip = resolve_host(host)
    if ip and is_ip_blacklisted(ip):
        # Пропускаем проверку, ключ сразу в мёртвые
        return None, None, None, None, key, ERR_OTHER

    if tag == "MY":
        fast_hint = get_country_fast(host, key)
        if fast_hint == "RU" and _has_many_ru_markers(host, key):
            return None, None, None, None, key, ERR_OTHER

    is_tls = (
        "security=tls" in key or
        "security=reality" in key or
        "trojan://" in key or
        "vmess://" in key
    )
    is_ws = "type=ws" in key or "net=ws" in key
    path = "/"
    match = re.search(r"path=([^&]+)", key)
    if match:
        path = unquote(match.group(1))

    start = time.time()
    error_kind = None

    try:
        if is_ws:
            protocol = "wss" if is_tls else "ws"
            ws_url = f"{protocol}://{host}:{port}{path}"
            ws = websocket.create_connection(
                ws_url,
                timeout=TIMEOUT,
                sslopt={"cert_reqs": ssl.CERT_NONE},
            )
            ws.close()
        elif is_tls:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            with socket.create_connection((host, port), timeout=TIMEOUT) as sock:
                with context.wrap_socket(sock, server_hostname=host):
                    pass
        else:
            with socket.create_connection((host, port), timeout=TIMEOUT):
                pass
    except socket.timeout:
        _inc_err(ERR_TIMEOUT)
        error_kind = ERR_TIMEOUT
        if ip:
            add_ip_to_blacklist(ip, f"timeout:{port}")
        return None, None, None, None, key, ERR_TIMEOUT
    except ssl.SSLError:
        _inc_err(ERR_TLS)
        error_kind = ERR_TLS
        if ip:
            add_ip_to_blacklist(ip, f"ssl_error:{port}")
        return None, None, None, None, key, ERR_TLS
    except socket.gaierror:
        _inc_err(ERR_DNS)
        error_kind = ERR_DNS
        if ip:
            add_ip_to_blacklist(ip, f"dns_error:{host}")
        return None, None, None, None, key, ERR_DNS
    except OSError as e:
        msg = str(e).lower()
        if "timed out" in msg or "timeout" in msg:
            _inc_err(ERR_TIMEOUT)
            if ip:
                add_ip_to_blacklist(ip, f"os_timeout:{port}")
            return None, None, None, None, key, ERR_TIMEOUT
        _inc_err(ERR_OTHER)
        if ip:
            add_ip_to_blacklist(ip, f"os_error:{str(e)[:50]}")
        return None, None, None, None, key, ERR_OTHER
    except Exception:
        _inc_err(ERR_OTHER)
        if ip:
            add_ip_to_blacklist(ip, "unknown_error")
        return None, None, None, None, key, ERR_OTHER

    # Успех — удаляем из чёрного списка, если был
    if ip:
        remove_ip_from_blacklist(ip)

    latency = int((time.time() - start) * 1000)
    country_exit = detect_exit_country_via_http(host)
    if country_exit == "UNKNOWN":
        country_exit = get_country_fast(host, key)
        if country_exit == "UNKNOWN":
            _inc_geo_stat("unknown")
        else:
            _inc_geo_stat("fast")
    return latency, tag, country_exit, host, key, None


# ==================== Форматирование / сохранение ====================

def make_final_key(k_id, latency, country):
    title_ru = country_to_title_ru(country)
    flag = country_to_flag(country)
    title_full = f"{title_ru} {country}" if country and country != "UNKNOWN" else title_ru
    info_str = f"[{latency}ms {title_full} {flag} {MY_CHANNEL}]"
    return f"{k_id}#{info_str}"


def extract_ping(key_str):
    try:
        label = key_str.split("#")[-1]
        match = re.search(r"(\d+)ms", label)
        if match:
            return int(match.group(1))
        return None
    except Exception:
        return None


def save_exact(keys, folder, filename):
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(keys) if keys else "")
    return path


def save_fixed_chunks_ru(keys_list, folder):
    valid_keys = [k.strip() for k in keys_list if k and k.strip()]
    chunks = [
        valid_keys[i:i + CHUNK_LIMIT]
        for i in range(0, min(len(valid_keys), CHUNK_LIMIT * 4), CHUNK_LIMIT)
    ]
    while len(chunks) < 4:
        chunks.append([])
    file_names = []
    for i, filename in enumerate(RU_FILES):
        save_exact(chunks[i] if i < len(chunks) else [], folder, filename)
        count = len(chunks[i]) if i < len(chunks) else 0
        print(f"  {filename}: {count} ключей")
        file_names.append(filename)
    return file_names


def save_fixed_chunks_euro(keys_list, folder):
    valid_keys = [k.strip() for k in keys_list if k and k.strip()]
    chunks = [
        valid_keys[i:i + EURO_CHUNK_LIMIT]
        for i in range(0, min(len(valid_keys), EURO_CHUNK_LIMIT * 3), EURO_CHUNK_LIMIT)
    ]
    while len(chunks) < 3:
        chunks.append([])
    file_names = []
    for i, filename in enumerate(EURO_FILES):
        save_exact(chunks[i] if i < len(chunks) else [], folder, filename)
        count = len(chunks[i]) if i < len(chunks) else 0
        print(f"  {filename}: {count} ключей")
        file_names.append(filename)
    return file_names


def save_chunked(keys_list, folder, base_name, chunk_size=None):
    if chunk_size is None:
        chunk_size = CHUNK_LIMIT
    valid_keys = [k.strip() for k in keys_list if k and k.strip()]
    chunks = [valid_keys[i:i + chunk_size] for i in range(0, len(valid_keys), chunk_size)]
    file_names = []
    for idx, chunk in enumerate(chunks, start=1):
        filename = f"{base_name}_part{idx}.txt"
        save_exact(chunk, folder, filename)
        file_names.append(filename)
        print(f"  {filename}: {len(chunk)} ключей")
    return file_names


def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def generate_subscriptions_list(ru_fast_files, ru_all_files, euro_fast_files, euro_all_files):
    GITHUB_USER_REPO = "sssergy/vpn-checker"
    BRANCH = "main"
    BASE_RAW = f"https://raw.githubusercontent.com/{GITHUB_USER_REPO}/{BRANCH}"
    subs_lines = []

    def nonempty_files(folder, filenames):
        out = []
        for fname in filenames:
            path = os.path.join(folder, fname)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                out.append(fname)
        return out

    ru_fast_nonempty = nonempty_files(FOLDER_RU, ru_fast_files)
    if ru_fast_nonempty:
        subs_lines.append("=== 🇷🇺 RUSSIA (FAST) ===")
        for filename in ru_fast_nonempty:
            subs_lines.append(f"{BASE_RAW}/checked/RU_Best/{filename}")
        subs_lines.append("")

    ru_all_nonempty = nonempty_files(FOLDER_RU, ru_all_files)
    if ru_all_nonempty:
        subs_lines.append("=== 🇷🇺 RUSSIA (ALL) ===")
        for fname in ru_all_nonempty:
            subs_lines.append(f"{BASE_RAW}/checked/RU_Best/{fname}")
        subs_lines.append("")

    euro_fast_nonempty = nonempty_files(FOLDER_EURO, euro_fast_files)
    if euro_fast_nonempty:
        subs_lines.append("=== 🇪🇺 EUROPE (FAST) ===")
        for filename in euro_fast_nonempty:
            subs_lines.append(f"{BASE_RAW}/checked/My_Euro/{filename}")
        subs_lines.append("")

    euro_all_nonempty = nonempty_files(FOLDER_EURO, euro_all_files)
    if euro_all_nonempty:
        subs_lines.append("=== 🇪🇺 EUROPE (ALL) ===")
        for fname in euro_all_nonempty:
            subs_lines.append(f"{BASE_RAW}/checked/My_Euro/{fname}")
        subs_lines.append("")

    ru_white_path = os.path.join(FOLDER_RU, "ru_white_all_WHITE.txt")
    if os.path.exists(ru_white_path) and os.path.getsize(ru_white_path) > 0:
        subs_lines.append("=== ✅ WHITE RUSSIA (ALL) ===")
        subs_lines.append(f"{BASE_RAW}/checked/RU_Best/ru_white_all_WHITE.txt")
        subs_lines.append("")

    euro_white_path = os.path.join(FOLDER_EURO, "my_euro_all_WHITE.txt")
    if os.path.exists(euro_white_path) and os.path.getsize(euro_white_path) > 0:
        subs_lines.append("=== ✅ WHITE EUROPE (ALL) ===")
        subs_lines.append(f"{BASE_RAW}/checked/My_Euro/my_euro_all_WHITE.txt")
        subs_lines.append("")

    ru_black_path = os.path.join(FOLDER_RU, "ru_white_all_BLACK.txt")
    if os.path.exists(ru_black_path) and os.path.getsize(ru_black_path) > 0:
        subs_lines.append("=== ⚠️ BLACK RUSSIA (ALL) ===")
        subs_lines.append(f"{BASE_RAW}/checked/RU_Best/ru_white_all_BLACK.txt")
        subs_lines.append("")

    euro_black_path = os.path.join(FOLDER_EURO, "my_euro_all_BLACK.txt")
    if os.path.exists(euro_black_path) and os.path.getsize(euro_black_path) > 0:
        subs_lines.append("=== ⚠️ BLACK EUROPE (ALL) ===")
        subs_lines.append(f"{BASE_RAW}/checked/My_Euro/my_euro_all_BLACK.txt")

    subs_path = os.path.join(BASE_DIR, "subscriptions_list.txt")
    with open(subs_path, "w", encoding="utf-8") as f:
        f.write("\n".join(subs_lines))

    http_count = sum(1 for l in subs_lines if l.startswith("http"))
    print(f"\n📋 subscriptions_list.txt создан ({http_count} ссылок):")
    for line in subs_lines:
        if line:
            print(f"  {line}")
    return subs_path


# ==================== MAIN ====================

if __name__ == "__main__":
    print("=== CHECKER v6 (FAST/ALL + WHITE/BLACK + GEO-CACHE + THROTTLE + SQLITE_BLACKLIST) ===")
    print(f"Параметры: CACHE={CACHE_HOURS}h, MAX_PING={MAX_PING_MS}ms, FAST={FAST_LIMIT}, HISTORY={MAX_HISTORY_AGE // 3600}h")
    print(f"Чёрный список SQLite: {BLACKLIST_DB}, блокировка на {BLACKLIST_DAYS} дней")

    # Очистка старого чёрного списка
    clean_old_blacklist()

    load_ip_cache()
    print(f"📂 Дисковый ip_cache загружен: {len(_disk_ip_cache)} записей")

    history = load_json(HISTORY_FILE)
    tasks = fetch_keys(URLS_RU, "RU") + fetch_keys(URLS_MY, "MY")

    unique_tasks = {k: tag for k, tag in tasks}
    all_items = list(unique_tasks.items())

    if len(all_items) > MAX_KEYS_TO_CHECK:
        all_items = all_items[:MAX_KEYS_TO_CHECK]

    current_time = time.time()
    to_check = []
    res_ru = []
    res_euro = []
    dead_ru = []
    dead_euro = []
    euro_filtered_ru = 0

    print(f"\n📊 Всего уникальных ключей: {len(all_items)}")

    for k, tag in all_items:
        k_id = k.split("#")[0]
        cached = history.get(k_id)
        if cached and (current_time - cached["time"] < CACHE_HOURS * 3600) and cached["alive"]:
            latency = cached["latency"]
            country = cached.get("country", "UNKNOWN")
            host = cached.get("host", "")
            final = make_final_key(k_id, latency, country)
            if tag == "RU":
                res_ru.append(final)
            elif tag == "MY":
                if is_russian_exit(k, host, country):
                    euro_filtered_ru += 1
                else:
                    res_euro.append(final)
        else:
            to_check.append((k, tag))

    print(f"✅ Из кэша: RU={len(res_ru)}, EURO={len(res_euro)}, EURO→RU filtered={euro_filtered_ru}")
    print(f"🔍 На проверку: {len(to_check)}")

    if to_check:
        checked_ok = 0
        total = len(to_check)
        print("Проверка: ", end="", flush=True)

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            future_map = {executor.submit(check_single_key, item): item for item in to_check}
            for future in as_completed(future_map):
                key, tag = future_map[future]
                try:
                    latency, _, country, host, original_key, err_type = future.result()
                except Exception:
                    if tag == "RU":
                        dead_ru.append(key)
                    else:
                        dead_euro.append(key)
                    checked_ok += 1
                    percent = int(checked_ok / total * 100)
                    print(f"\rПроверка: {percent}% ({checked_ok}/{total})", end="", flush=True)
                    continue

                if latency is None:
                    if tag == "RU":
                        dead_ru.append(original_key)
                    elif tag == "MY":
                        dead_euro.append(original_key)
                else:
                    k_id = original_key.split("#")[0]
                    history[k_id] = {
                        "alive": True,
                        "latency": latency,
                        "time": time.time(),
                        "country": country,
                        "host": host,
                    }
                    final = make_final_key(k_id, latency, country)
                    if tag == "RU":
                        res_ru.append(final)
                    elif tag == "MY":
                        if is_russian_exit(original_key, host, country):
                            euro_filtered_ru += 1
                            dead_euro.append(original_key)
                        else:
                            res_euro.append(final)

                checked_ok += 1
                percent = int(checked_ok / total * 100)
                print(f"\rПроверка: {percent}% ({checked_ok}/{total})", end="", flush=True)

        print()
        print(f"✅ Проверено успешно: {checked_ok}")

    save_ip_cache()
    print(f"💾 ip_cache сохранён: {len(_disk_ip_cache)} записей")

    save_json(
        HISTORY_FILE,
        {k: v for k, v in history.items() if current_time - v["time"] < MAX_HISTORY_AGE}
    )

    res_ru_clean = [k for k in res_ru if extract_ping(k) is not None and extract_ping(k) <= MAX_PING_MS]
    res_euro_clean = [k for k in res_euro if extract_ping(k) is not None and extract_ping(k) <= MAX_PING_MS]

    res_ru_clean.sort(key=extract_ping)
    res_euro_clean.sort(key=extract_ping)

    print(f"\n📈 После фильтрации (≤ {MAX_PING_MS} ms) и сортировки:")
    print(f"  RU: {len(res_ru_clean)} ключей")
    print(f"  EURO: {len(res_euro_clean)} ключей")

    res_ru_fast = res_ru_clean[:FAST_LIMIT]
    res_euro_fast = res_euro_clean[:FAST_LIMIT]

    print(f"\n🚀 FAST слои (топ {FAST_LIMIT}):")
    print(f"  RU FAST: {len(res_ru_fast)}")
    print(f"  EURO FAST: {len(res_euro_fast)}")

    print(f"\n💾 Сохранение RU FAST → {FOLDER_RU}:")
    ru_fast_files = save_fixed_chunks_ru(res_ru_fast, FOLDER_RU)

    print(f"\n💾 Сохранение EURO FAST → {FOLDER_EURO} (по {EURO_CHUNK_LIMIT} ключей):")
    euro_fast_files = save_fixed_chunks_euro(res_euro_fast, FOLDER_EURO)

    print(f"\n💾 Сохранение RU ALL → {FOLDER_RU}:")
    ru_all_files = save_chunked(res_ru_clean, FOLDER_RU, "ru_white_all")

    print(f"\n💾 Сохранение EURO ALL → {FOLDER_EURO} (по {EURO_CHUNK_LIMIT} ключей):")
    euro_all_files = save_chunked(res_euro_clean, FOLDER_EURO, "my_euro_all", chunk_size=EURO_CHUNK_LIMIT)

    print(f"\n💾 WHITE/BLACK → {FOLDER_RU}:")
    save_exact(res_ru_clean, FOLDER_RU, "ru_white_all_WHITE.txt")
    save_exact(dead_ru, FOLDER_RU, "ru_white_all_BLACK.txt")

    print(f"\n💾 WHITE/BLACK → {FOLDER_EURO}:")
    save_exact(res_euro_clean, FOLDER_EURO, "my_euro_all_WHITE.txt")
    save_exact(dead_euro, FOLDER_EURO, "my_euro_all_BLACK.txt")

    generate_subscriptions_list(ru_fast_files, ru_all_files, euro_fast_files, euro_all_files)

    print("\n" + "=" * 55)
    print("📊 ФИНАЛЬНЫЙ ОТЧЁТ")
    print("=" * 55)

    print(f"\n✅ Результат:")
    print(f"  RU FAST: {len(res_ru_fast)}, RU WHITE: {len(res_ru_clean)}, RU BLACK: {len(dead_ru)}")
    print(f"  EURO FAST: {len(res_euro_fast)}, EURO WHITE: {len(res_euro_clean)}, EURO BLACK: {len(dead_euro)}")
    print(f"  EURO ключей отфильтровано как RU-exit: {euro_filtered_ru}")

    print(f"\n🌍 Источник страны (geo-статистика):")
    with _geo_stats_lock:
        stats = dict(_geo_stats)
    total_geo = sum(stats.values()) or 1
    for src in ("api", "cache", "fast", "unknown"):
        n = stats.get(src, 0)
        print(f"  {src:8s}: {n:5d}  ({n * 100 // total_geo}%)")
    if _ip_api_disabled:
        print("  ⚠️  ip-api был отключён из-за 429 в процессе работы")

    print(f"\n❌ Ошибки соединения:")
    with _err_stats_lock:
        estats = dict(_err_stats)
    total_err = sum(estats.values()) or 1
    for kind in (ERR_TIMEOUT, ERR_TLS, ERR_DNS, ERR_OTHER):
        n = estats.get(kind, 0)
        print(f"  {kind:8s}: {n:5d}  ({n * 100 // total_err}%)")

    # Вывод статистики чёрного списка
    conn = get_blacklist_conn()
    cur = conn.execute("SELECT COUNT(*) FROM blacklist")
    blacklist_count = cur.fetchone()[0]
    print(f"\n🚫 Чёрный список SQLite: {blacklist_count} IP заблокировано на {BLACKLIST_DAYS} дней")

    print("\n✅ SUCCESS: FAST/ALL + WHITE/BLACK GENERATED")