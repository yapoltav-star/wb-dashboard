"""Parse pasted giveaway plans without treating prices or model numbers as quantities."""
import re
from datetime import date

HEADER = re.compile(r"^(?:0[.,]\d{2}|\d{2,4})[ _-]*[a-zа-яё][a-zа-яё _-]{1,60}$", re.I)
DAY = re.compile(r"^(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?\.?$")
QUANTITY = re.compile(r"(?<!\d)(\d+)\s*(?:штук|штуки|шт)(?:\.)?(?![а-яё])", re.I)
ARTICLE = re.compile(r"(?<!\d)\d{6,12}(?!\d)")
MAX_ENTRIES = 30


def parse_day(text, year):
    match = DAY.fullmatch(text.strip())
    if not match:
        raise ValueError("Нужна дата ДД.ММ или ДД.ММ.ГГГГ.")
    day, month, explicit_year = match.groups()
    try:
        return date(int(explicit_year or year), int(month), int(day)).isoformat()
    except ValueError:
        raise ValueError("Такой даты нет в календаре.") from None


def looks_like_batch(text):
    return ("\n" in text and QUANTITY.search(text) is not None
            and any(HEADER.fullmatch(line.strip().strip("\\").strip()) for line in text.replace("\xa0", " ").splitlines()))


def parse_batch(text, year):
    entries, errors = [], []
    title, planned_date, pending = "", "", []

    def flush():
        nonlocal pending
        if not pending:
            return
        line = " ".join(pending)
        pending = []
        quantities = list(QUANTITY.finditer(line))
        if not title:
            errors.append("Перед заданием нужно название своего товара.")
            return
        if len(quantities) != 1:
            errors.append(f"{title}: в каждой строке задания нужно одно количество с «шт».")
            return
        quantity = quantities[0]
        plan = int(quantity.group(1))
        if not 1 <= plan <= 500:
            errors.append(f"{title}: количество должно быть от 1 до 500 шт.")
            return
        before = line[:quantity.start()].strip(" ,;—–-")
        after = line[quantity.end():].strip(" ,;—–-")
        articles = list(dict.fromkeys(ARTICLE.findall(before)))
        if articles:
            if len(articles) != 1:
                errors.append(f"{title}: найдено несколько артикулов конкурента, раздели задания.")
                return
            channel, query = "competitor", articles[0]
            keyword = re.sub(r"^по\s+ключу\s*[:—-]?\s*", "", after, flags=re.I)
        else:
            key = re.search(r"\bпо\s+ключу\s*[:—-]?\s*(.+)", after, re.I)
            if before or not key:
                errors.append(f"{title}: нужен артикул конкурента или «N шт по ключу …».")
                return
            channel, query, keyword = "keyword", key.group(1).strip(), ""
        if len(query) > 80 or len(keyword) > 80:
            errors.append(f"{title}: поисковый ключ длиннее 80 символов.")
            return
        entries.append({"title": title, "nm": "", "channel": channel,
                        "query": query, "keyword": keyword, "plan": plan,
                        "planned_date": planned_date})

    for raw in text.replace("\xa0", " ").splitlines():
        line = raw.strip().strip("\\").strip()
        if not line:
            continue
        if HEADER.fullmatch(line):
            flush()
            title, planned_date = line, ""
        elif DAY.fullmatch(line):
            flush()
            try:
                planned_date = parse_day(line, year)
            except ValueError as exc:
                errors.append(f"{title or 'Дата'}: {line}: {exc}")
                planned_date = ""
        else:
            if QUANTITY.search(line) and any(QUANTITY.search(p) for p in pending):
                flush()
            pending.append(line)
    flush()
    if len(entries) > MAX_ENTRIES:
        errors.append(f"В одном сообщении допускается до {MAX_ENTRIES} раздач. Раздели список.")
    if not entries and not errors:
        errors.append("Не нашёл заданий с количеством в штуках.")
    return entries, errors
