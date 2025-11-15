import time
import pickle
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, List, Optional, Set, Dict, Tuple
from urllib.parse import urlparse

from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from urlextract import URLExtract
import requests


# ==========================
# Конфігурація програми
# ==========================

# Файл з даними для парсингу
DATA_FILE = Path("data") / "messages_to_parse.dat"

# Ліміт кількості записів для обробки.
# None  -> обробляємо всі записи
# число -> обробляємо тільки перші N записів
DEBUG_LIMIT: Optional[int] = 100
# DEBUG_LIMIT: Optional[int] = None
# Для фінального запуску можна буде поставити:
# DEBUG_LIMIT = None

# Режим роботи з unshorten URL:
# "6.1"  -> тільки підхід зі списком коротювачів
# "6.2"  -> перевіряти всі URL на редіректи
# "both" -> (для експериментів) працює як "6.2", але в логах показує статистику по коротювачах
# UNSHORTEN_MODE: str = "6.1"
# UNSHORTEN_MODE: str = "6.2"
UNSHORTEN_MODE: str = "both"

# Кількість потоків для перевірки URL
MAX_WORKERS: int = 10

# Таймаут для HTTP-запитів у секундах
REQUEST_TIMEOUT: int = 5

# Відомі домени-коротювачі (для режиму 6.1)
SHORTENER_DOMAINS = {
    "bit.ly",
    "t.co",
    "goo.gl",
    "tinyurl.com",
    "ow.ly",
    "buff.ly",
    "bitly.com",
    "is.gd",
    "buff.ly",
    "tiny.cc",
    "ow.ly",
    "newspr.es",
    "ti.me",
}


# ==========================
# Налаштування логування
# ==========================

def setup_logging() -> None:
    """Налаштувати loguru: логування в файл з ротацією кожні 5 хв та ретеншном 20 хв."""
    logger.remove()

    # Лог у консоль (простий варіант)
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
    )

    # Лог у файл: новий файл кожні 5 хвилин, старші 20 хв видаляються
    Path("logs").mkdir(exist_ok=True)
    logger.add(
        "logs/app_{time}.log",
        rotation="5 minutes",
        retention="20 minutes",
        level="INFO",
        encoding="utf-8",
        enqueue=True,
    )


# ==========================
# Робота з даними
# ==========================

def load_messages(path: Path) -> Any:
    """
    Завантажити дані з pickle-файлу.

    Повертає те, що лежить всередині messages_to_parse.dat.
    """
    logger.info(f"Спроба завантажити дані з файлу: {path}\n")

    if not path.exists():
        logger.error(f"Файл не знайдено: {path}\n")
        raise FileNotFoundError(f"Файл не знайдено: {path}")

    with path.open("rb") as f:
        data = pickle.load(f)

    logger.info(
        f"Дані успішно завантажено. Тип об'єкта: {type(data)}\n"
    )
    return data


def apply_debug_limit(items: Iterable[Any], limit: Optional[int]) -> List[Any]:
    """
    Застосувати DEBUG_LIMIT до послідовності елементів.

    Якщо limit is None -> повертаємо всі елементи.
    Якщо число        -> повертаємо тільки перші limit.
    """
    items_list = list(items)

    if limit is None:
        logger.info(
            f"DEBUG_LIMIT не встановлений. Обробляємо всі {len(items_list)} елемент(и/ів).\n"
        )
        return items_list

    limited = items_list[:limit]
    logger.info(
        f"DEBUG_LIMIT = {limit}. "
        f"Буде оброблено {len(limited)} елемент(и/ів) з {len(items_list)}.\n"
    )
    return limited


# ==========================
# Витягування URL з повідомлень
# ==========================

def extract_urls_from_messages(messages: Iterable[Any]) -> List[str]:
    """
    Пройтись по всіх повідомленнях, витягнути всі URL за допомогою URLExtract.

    Повертає відсортований список унікальних URL (без дублікатів).
    """
    extractor = URLExtract()
    all_urls: Set[str] = set()

    logger.info("Починаю витягувати URL з повідомлень...\n")

    for idx, msg in enumerate(messages, start=1):
        text = str(msg)
        found_urls = extractor.find_urls(text)

        if found_urls:
            all_urls.update(found_urls)
            logger.debug(
                f"[extract_urls_from_messages] Повідомлення #{idx}: "
                f"знайдено {len(found_urls)} URL.\n"
            )

    urls_list = sorted(all_urls)

    logger.info(f"Всього знайдено унікальних URL: {len(urls_list)}\n")

    # Покажемо кілька перших для наочності
    preview_count = min(10, len(urls_list))
    if preview_count > 0:
        preview = urls_list[:preview_count]
        logger.info(
            f"Перші {preview_count} URL:\n"
            + "\n".join(f"  - {u}" for u in preview)
            + "\n"
        )
    else:
        logger.info("URL у повідомленнях не знайдено.\n")

    return urls_list


# ==========================
# Перевірка доступності URL
# ==========================

def normalize_url(url: str) -> str:
    """
    Додати схему http:// якщо її немає.

    Приклад:
      - 'Realtor.com'      -> 'http://Realtor.com'
      - 'http://example'   -> без змін
      - 'https://example'  -> без змін
    """
    if url.startswith(("http://", "https://")):
        return url
    return "http://" + url


def check_url_status(url: str) -> Tuple[str, Optional[int]]:
    """
    Перевірити один URL за допомогою requests.head.

    Повертає кортеж (оригінальний_url, status_code або None у разі помилки).
    """
    normalized = normalize_url(url)

    try:
        response = requests.head(
            normalized,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        status_code = response.status_code
        logger.debug(f"[check_url_status] {url} -> {status_code}\n")
        return url, status_code
    except requests.RequestException as e:
        logger.warning(f"[check_url_status] Помилка для {url}: {e}\n")
        return url, None


def build_url_status_dict(urls: List[str]) -> Dict[str, Optional[int]]:
    """
    Перевірити список URL у потоках і сформувати словник:
      ключ     -> оригінальний URL
      значення -> status_code (або None, якщо була помилка)
    """
    if not urls:
        logger.info("Список URL порожній, перевіряти нічого.\n")
        return {}

    logger.info(
        f"Починаю перевірку доступності URL. "
        f"Кількість URL для перевірки: {len(urls)}\n"
    )

    max_workers = min(MAX_WORKERS, len(urls))
    results: Dict[str, Optional[int]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(check_url_status, url): url for url in urls}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                orig_url, status_code = future.result()
                results[orig_url] = status_code
            except Exception as e:
                logger.exception(f"[build_url_status_dict] Непередбачена помилка для {url}: {e}\n")
                results[url] = None

    # Трохи статистики
    successful = sum(1 for s in results.values() if isinstance(s, int) and 200 <= s < 400)
    failed = sum(1 for s in results.values() if s is None or not (200 <= (s or 0) < 400))

    logger.info(
        f"Перевірка URL завершена. "
        f"Успішних (2xx-3xx): {successful}, "
        f"з помилками/ін. статусом: {failed}.\n"
    )

    # Покажемо кілька прикладів
    preview_items = list(results.items())[: min(10, len(results))]
    if preview_items:
        logger.info("Приклади результатів перевірки URL:\n")
        for url, status in preview_items:
            logger.info(f"  - {url} -> {status}\n")

    return results


# ==========================
# Визначення скорочених URL + unshorten
# ==========================

def is_shortened_url(url: str) -> bool:
    """
    Перевірити, чи URL належить відомому коротювачу (по домену).
    Використовується в режимі 6.1.
    """
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()

    if host.startswith("www."):
        host = host[4:]

    return host in SHORTENER_DOMAINS


def unshorten_url(url: str) -> Tuple[str, str]:
    """
    Розкрити (unshorten) один URL.

    Повертає (оригінальний_url, фінальний_url).
    Якщо помилка — фінальним вважаємо нормалізований варіант.
    """
    normalized = normalize_url(url)

    try:
        response = requests.get(
            normalized,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        final_url = response.url
        logger.debug(f"[unshorten_url] {url} -> {final_url}\n")
        return url, final_url
    except requests.RequestException as e:
        logger.warning(f"[unshorten_url] Помилка для {url}: {e}\n")
        return url, normalized


def build_unshorten_mapping(urls: List[str], mode: str) -> Dict[str, str]:
    """
    Побудувати словник:
        оригінальний URL -> розкритий (фінальний) URL

    Залежить від режиму:
      - "6.1": тільки коротювачі зі списку SHORTENER_DOMAINS
      - "6.2": всі URL
      - "both": зараз працює як "6.2", але додатково логуються
                скільки URL потрапляють під 6.1.
    """
    if not urls:
        logger.info("Список URL порожній, розкривати нічого.\n")
        return {}

    # Визначаємо, які URL будемо розкривати
    shortened_urls = [u for u in urls if is_shortened_url(u)]

    if mode == "6.1":
        target_urls = shortened_urls
        logger.info(
            f"UNSHORTEN_MODE=6.1. Будемо розкривати тільки коротювачі зі списку. "
            f"Кількість таких URL: {len(target_urls)} з {len(urls)} загальних.\n"
        )
    elif mode in ("6.2", "both"):
        target_urls = urls
        logger.info(
            f"UNSHORTEN_MODE={mode}. Будемо розкривати ВСІ URL. "
            f"З них {len(shortened_urls)} виглядають як коротювачі зі списку.\n"
        )
        if mode == "both":
            logger.info(
                "Режим 'both' зараз працює як '6.2' (усі URL). "
                "Для замірів часу кожного підходу окремо можна "
                "запускати програму з UNSHORTEN_MODE='6.1' та '6.2'.\n"
            )
    else:
        logger.warning(
            f"Невідомий UNSHORTEN_MODE={mode}. "
            f"За замовчуванням розкриватимемо всі URL (як 6.2).\n"
        )
        target_urls = urls

    if not target_urls:
        logger.info("Немає URL для розкриття у вибраному режимі.\n")
        return {}

    logger.info(
        f"Починаю unshorten для {len(target_urls)} URL "
        f"(усього URL у списку: {len(urls)}).\n"
    )

    max_workers = min(MAX_WORKERS, len(target_urls))
    results: Dict[str, str] = {}

    start_unshorten = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(unshorten_url, url): url for url in target_urls}

        for future in as_completed(future_to_url):
            orig_url, final_url = future.result()
            results[orig_url] = final_url

    end_unshorten = time.perf_counter()
    duration = end_unshorten - start_unshorten

    logger.info(
        f"Розкриття URL завершено. Кількість розкритих URL: {len(results)}. "
        f"Час виконання блоку unshorten: {duration:.3f} сек.\n"
    )

    # Покажемо кілька прикладів
    preview_items = list(results.items())[: min(10, len(results))]
    if preview_items:
        logger.info("Приклади результатів розкриття URL:\n")
        for orig, final in preview_items:
            logger.info(f"  - {orig} -> {final}\n")

    return results


# ==========================
# Головна функція
# ==========================

def main() -> None:
    setup_logging()
    logger.info("=== Старт програми ===\n")
    logger.info(
        f"CONFIG: DATA_FILE={DATA_FILE}, DEBUG_LIMIT={DEBUG_LIMIT}, "
        f"UNSHORTEN_MODE={UNSHORTEN_MODE}, MAX_WORKERS={MAX_WORKERS}\n"
    )

    start_time = time.perf_counter()

    # 1. Завантажуємо дані з pickle
    try:
        raw_data = load_messages(DATA_FILE)
    except Exception as e:
        logger.exception(f"Помилка при завантаженні даних: {e}\n")
        return

    # 2. Пробуємо привести дані до послідовності "повідомлень"
    if isinstance(raw_data, (list, tuple)):
        messages_seq = raw_data
        logger.info(f"Завантажено послідовність з {len(messages_seq)} елемент(ів).\n")
    else:
        logger.warning(
            "Формат даних не є списком/кортежем. "
            "Далі адаптуємо обробку під реальну структуру.\n"
        )
        messages_seq = [raw_data]

    # 3. Застосовуємо DEBUG_LIMIT
    messages = apply_debug_limit(messages_seq, DEBUG_LIMIT)

    logger.info(f"Готово до подальшої обробки. Кількість повідомлень: {len(messages)}\n")

    # 4. Витягуємо всі URL з повідомлень
    urls = extract_urls_from_messages(messages)
    logger.info(f"Після витягання URL маємо {len(urls)} унікальн(ий/их) URL.\n")

    # 5. Перевіряємо доступність кожного URL (requests.head + threading)
    url_status_dict = build_url_status_dict(urls)
    logger.info(
        f"Словник url -> status_code сформовано. "
        f"Кількість ключів: {len(url_status_dict)}\n"
    )

    # === ПУНКТ 4 ЗАВДАННЯ ===
    # 1-й довідник: оригінальний URL -> status_code
    original_to_status = url_status_dict

    # 2-й довідник: оригінальний URL -> розкритий (фінальний) URL
    original_to_unshortened = build_unshorten_mapping(urls, UNSHORTEN_MODE)

    logger.info(
        "Довідники для пункту 4 сформовані:\n"
        f"  - original_to_status: {len(original_to_status)} ключів\n"
        f"  - original_to_unshortened: {len(original_to_unshortened)} ключів\n"
    )

    end_time = time.perf_counter()
    duration = end_time - start_time
    logger.info(f"=== Завершення програми. Час виконання: {duration:.3f} сек. ===\n")


if __name__ == "__main__":
    main()
