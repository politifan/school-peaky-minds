import re
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from core import (
    LECTURE_DOCUMENTS_DIR,
    LECTURE_RECORDS_FILE,
    build_month_calendar,
    load_json,
    month_key,
    moscow_now,
    save_json,
)


ACCOUNT_API_ENDPOINTS = {
    "schedule": "/api/account/schedule",
    "lectures": "/api/account/lectures",
}

ACCOUNT_SECTION_META = {
    "schedule": {
        "kicker": "Schedule",
        "title": "Расписание",
        "empty_title": "Расписание пока пустое",
        "empty_text": "Когда курс и занятия будут назначены, ближайшие события появятся здесь и в API кабинета.",
    },
    "lectures": {
        "kicker": "Lectures",
        "title": "Мои лекции",
        "empty_title": "Записей лекций пока нет",
        "empty_text": "Когда преподаватель загрузит первую запись, она появится здесь автоматически.",
    },
}

SCHEDULE_STATUS_META = {
    "proposed": {
        "label": "Предложено",
        "event_title": "Предварительное занятие",
        "kind": "proposal",
    },
    "approved": {
        "label": "Согласовано",
        "event_title": "Подтверждённое занятие",
        "kind": "lesson",
    },
    "missed": {
        "label": "Пропуск",
        "event_title": "Пропущенное занятие",
        "kind": "missed",
    },
    "excused": {
        "label": "Уважительная причина",
        "event_title": "Перенесённое занятие",
        "kind": "excused",
    },
}

LECTURE_ALLOWED_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".m4v",
    ".webm",
    ".mp3",
    ".m4a",
    ".ogg",
    ".wav",
    ".pdf",
}


def _format_schedule_dt(value: datetime) -> str:
    return value.strftime("%d.%m · %H:%M")


def _format_schedule_date_label(value: date, time_value: str) -> str:
    if time_value:
        return f"{value.strftime('%d.%m')} · {time_value}"
    return f"{value.strftime('%d.%m')} · время уточняется"


def _format_published_label(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return value
    return dt.strftime("%d.%m.%Y")


def _normalize_month(value: str) -> str:
    month_value = str(value or "").strip()
    if re.match(r"^\d{4}-\d{2}$", month_value):
        return month_value
    return month_key()


def _normalize_time_value(value: Any) -> str:
    raw = str(value or "").strip()
    if re.match(r"^\d{2}:\d{2}:\d{2}$", raw):
        raw = raw[:5]
    if re.match(r"^\d{2}:\d{2}$", raw):
        return raw
    return ""


def _parse_date_key(value: str) -> Optional[date]:
    raw = str(value or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def _extract_lesson_status_map(item: Dict[str, Any]) -> Dict[str, Any]:
    lesson_calendar_map = item.get("lesson_calendar_map")
    if isinstance(lesson_calendar_map, dict):
        return lesson_calendar_map
    lesson_calendar = item.get("lesson_calendar")
    if isinstance(lesson_calendar, dict):
        return lesson_calendar
    return {}


def _lecture_public_url(file_name: str) -> str:
    return f"/documents/lectures/{file_name}"


def _is_valid_lecture_url(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    if raw.startswith("/documents/lectures/"):
        return True
    parsed = urlparse(raw)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_duration_label(value: Any) -> str:
    raw = str(value or "").strip()
    return raw or "60 минут"


def _normalize_lecture_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    record_id = str(item.get("id") or "").strip() or f"lecture-{secrets.token_hex(8)}"
    agreement_file = str(item.get("agreement_file") or "").strip()
    title = str(item.get("title") or "").strip()
    if not agreement_file or not title:
        return None

    file_name = str(item.get("file_name") or "").strip()
    url = str(item.get("url") or "").strip()
    if file_name and not url:
        url = _lecture_public_url(file_name)
    if not _is_valid_lecture_url(url):
        return None

    published_at = str(item.get("published_at") or "").strip() or moscow_now().isoformat()
    created_at = str(item.get("created_at") or "").strip() or published_at
    updated_at = str(item.get("updated_at") or "").strip() or created_at
    source_type = str(item.get("source_type") or "").strip()
    if source_type not in {"upload", "link"}:
        source_type = "upload" if file_name else "link"

    return {
        "id": record_id,
        "agreement_file": agreement_file,
        "course": str(item.get("course") or "").strip(),
        "title": title,
        "description": str(item.get("description") or "").strip(),
        "duration_label": _normalize_duration_label(item.get("duration_label")),
        "teacher": str(item.get("teacher") or "").strip() or "Преподаватель Peaky Minds",
        "published_at": published_at,
        "published_label": _format_published_label(published_at),
        "source_type": source_type,
        "source_label": "Файл на сервере" if source_type == "upload" else "Внешняя ссылка",
        "url": url,
        "file_name": file_name,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def load_lecture_records() -> List[Dict[str, Any]]:
    data = load_json(LECTURE_RECORDS_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        raw_items = []

    items: List[Dict[str, Any]] = []
    seen = set()
    for raw_item in raw_items:
        normalized = _normalize_lecture_record(raw_item)
        if not normalized:
            continue
        if normalized["id"] in seen:
            continue
        seen.add(normalized["id"])
        items.append(normalized)

    items.sort(key=lambda item: (item["published_at"], item["updated_at"], item["id"]), reverse=True)
    return items


def save_lecture_records(items: List[Dict[str, Any]]) -> None:
    save_json(LECTURE_RECORDS_FILE, {"items": items})


def get_lecture_record(record_id: str) -> Optional[Dict[str, Any]]:
    record_key = str(record_id or "").strip()
    if not record_key:
        return None
    for item in load_lecture_records():
        if item["id"] == record_key:
            return item
    return None


def list_lecture_records(
    *,
    agreement_file: str = "",
    agreement_files: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    single = str(agreement_file or "").strip()
    allowed = {single} if single else set()
    if agreement_files:
        allowed.update(str(item or "").strip() for item in agreement_files if str(item or "").strip())
    items = load_lecture_records()
    if not allowed:
        return items
    return [item for item in items if item["agreement_file"] in allowed]


def save_lecture_upload(upload: Any) -> Tuple[str, str]:
    file_name = str(getattr(upload, "filename", "") or "").strip()
    if not file_name:
        raise ValueError("Не выбран файл записи")

    ext = Path(file_name).suffix.lower()
    if ext not in LECTURE_ALLOWED_EXTENSIONS:
        raise ValueError("Недопустимый формат записи")

    saved_name = f"lecture_{moscow_now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(6)}{ext}"
    target = LECTURE_DOCUMENTS_DIR / saved_name
    upload.file.seek(0)
    target.write_bytes(upload.file.read())
    return saved_name, _lecture_public_url(saved_name)


def delete_lecture_file(file_name: str) -> None:
    raw = str(file_name or "").strip()
    if not raw:
        return
    target = LECTURE_DOCUMENTS_DIR / raw
    try:
        if target.exists():
            target.unlink()
    except Exception:
        return


def upsert_lecture_record(
    *,
    agreement_file: str,
    course: str,
    title: str,
    description: str = "",
    duration_label: str = "",
    teacher: str = "",
    published_at: str = "",
    url: str = "",
    upload: Any = None,
    record_id: str = "",
) -> Dict[str, Any]:
    agreement_key = str(agreement_file or "").strip()
    title_value = str(title or "").strip()
    if not agreement_key:
        raise ValueError("Не указан договор для записи")
    if not title_value:
        raise ValueError("Укажите название записи")

    items = load_lecture_records()
    existing = None
    record_key = str(record_id or "").strip()
    if record_key:
        for item in items:
            if item["id"] == record_key:
                existing = item
                break

    upload_name = str(getattr(upload, "filename", "") or "").strip()
    saved_file_name = ""
    saved_url = ""
    source_type = "link"

    url_value = str(url or "").strip()
    if upload_name:
        saved_file_name, saved_url = save_lecture_upload(upload)
        source_type = "upload"
        url_value = saved_url
    elif url_value:
        if not _is_valid_lecture_url(url_value):
            raise ValueError("Укажите корректную ссылку на запись")
        source_type = "link"
    elif existing:
        url_value = existing["url"]
        saved_file_name = existing.get("file_name") or ""
        source_type = existing.get("source_type") or ("upload" if saved_file_name else "link")
    else:
        raise ValueError("Укажите ссылку или загрузите файл записи")

    if existing and existing.get("file_name") and (
        source_type != existing.get("source_type") or saved_file_name != existing.get("file_name")
    ):
        delete_lecture_file(existing.get("file_name") or "")

    now_iso = moscow_now().isoformat()
    payload = _normalize_lecture_record(
        {
            "id": record_key or f"lecture-{secrets.token_hex(8)}",
            "agreement_file": agreement_key,
            "course": str(course or "").strip() or (existing.get("course") if existing else ""),
            "title": title_value,
            "description": str(description or "").strip(),
            "duration_label": duration_label,
            "teacher": teacher,
            "published_at": str(published_at or "").strip() or (existing.get("published_at") if existing else now_iso),
            "source_type": source_type,
            "url": url_value,
            "file_name": saved_file_name,
            "created_at": existing.get("created_at") if existing else now_iso,
            "updated_at": now_iso,
        }
    )
    if not payload:
        raise ValueError("Не удалось сохранить запись")

    updated_items = [item for item in items if item["id"] != payload["id"]]
    updated_items.append(payload)
    save_lecture_records(updated_items)
    return payload


def delete_lecture_record(record_id: str) -> bool:
    record_key = str(record_id or "").strip()
    if not record_key:
        return False

    items = load_lecture_records()
    removed = None
    updated_items = []
    for item in items:
        if item["id"] == record_key:
            removed = item
            continue
        updated_items.append(item)

    if not removed:
        return False

    if removed.get("file_name"):
        delete_lecture_file(removed.get("file_name") or "")
    save_lecture_records(updated_items)
    return True


def build_account_schedule_items(agreements: List[Dict[str, Any]], *, limit: int = 12) -> List[Dict[str, Any]]:
    today = moscow_now().date()
    items: List[Dict[str, Any]] = []

    for agreement in agreements:
        course = str(agreement.get("course") or "").strip() or "Курс"
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or "").strip()
        teacher = str(agreement.get("teacher") or "").strip() or "Преподаватель Peaky Minds"
        for date_key, entry in _extract_lesson_status_map(agreement).items():
            date_value = _parse_date_key(date_key)
            if not date_value or date_value < today:
                continue

            if isinstance(entry, dict):
                status = str(entry.get("status") or "").strip()
                time_value = _normalize_time_value(entry.get("time"))
            else:
                status = str(entry or "").strip()
                time_value = ""

            if status not in {"proposed", "approved"}:
                continue

            hour = int(time_value[:2]) if time_value else 19
            minute = int(time_value[3:5]) if time_value else 0
            starts_at = datetime(
                year=date_value.year,
                month=date_value.month,
                day=date_value.day,
                hour=hour,
                minute=minute,
            )
            meta = SCHEDULE_STATUS_META[status]
            items.append(
                {
                    "id": f"{agreement_file}:{date_key}",
                    "agreement_file": agreement_file,
                    "course": course,
                    "title": meta["event_title"],
                    "starts_at": starts_at.isoformat(),
                    "starts_label": _format_schedule_date_label(date_value, time_value),
                    "duration_label": "60 минут",
                    "channel": "Онлайн",
                    "teacher": teacher,
                    "kind": meta["kind"],
                    "status": status,
                    "_sort_key": starts_at.timestamp(),
                }
            )

    items.sort(key=lambda item: (item["_sort_key"], item["course"], item["id"]))
    for item in items:
        item.pop("_sort_key", None)
    return items[:limit]


def build_account_schedule_payload(agreements: List[Dict[str, Any]], *, month: str = "") -> Dict[str, Any]:
    month_value = _normalize_month(month)
    upcoming_items = build_account_schedule_items(agreements)
    next_by_agreement = {item["agreement_file"]: item for item in upcoming_items if item.get("agreement_file")}
    calendars = []

    for agreement in agreements:
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or "").strip()
        course = str(agreement.get("course") or "").strip() or "Курс"
        calendar_map = _extract_lesson_status_map(agreement)
        calendar_weeks = build_month_calendar(month_value, calendar_map)
        month_events = 0
        for date_key, entry in calendar_map.items():
            if not date_key.startswith(month_value):
                continue
            if isinstance(entry, dict):
                status = str(entry.get("status") or "").strip()
            else:
                status = str(entry or "").strip()
            if status in SCHEDULE_STATUS_META:
                month_events += 1
        calendars.append(
            {
                "agreement_file": agreement_file,
                "course": course,
                "month": month_value,
                "calendar": calendar_weeks,
                "items_count": month_events,
                "next_event": next_by_agreement.get(agreement_file),
            }
        )

    return {
        "month": month_value,
        "legend": [
            {"key": key, "label": meta["label"]}
            for key, meta in SCHEDULE_STATUS_META.items()
        ],
        "items": upcoming_items,
        "calendars": calendars,
        "total": len(upcoming_items),
    }


def build_account_lecture_items(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agreement_map = {}
    for item in agreements:
        agreement_file = str(item.get("agreement_file") or item.get("_file") or "").strip()
        if not agreement_file:
            continue
        agreement_map[agreement_file] = str(item.get("course") or "").strip() or "Курс"

    if not agreement_map:
        return []

    items = []
    for record in list_lecture_records(agreement_files=list(agreement_map.keys())):
        items.append(
            {
                **record,
                "course": record.get("course") or agreement_map.get(record["agreement_file"]) or "Курс",
            }
        )
    return items


def build_account_lectures_payload(agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    items = build_account_lecture_items(agreements)
    return {
        "items": items,
        "total": len(items),
        "storage": {
            "mode": "server-filesystem",
            "public_base_url": "/documents/lectures/",
        },
    }


def _build_account_hero(
    *,
    user_display: str,
    has_agreements: bool,
    payments_enabled: bool,
    total_courses: int,
    signed_contracts: int,
    upcoming_events: int,
    lecture_records: int,
) -> Dict[str, Any]:
    return {
        "kicker": "Student workspace",
        "title": "Личный кабинет",
        "user_display": user_display,
        "lead": "Здесь собраны договоры, прогресс по курсам, материалы, календарь и записи лекций.",
        "summary": [
            "Договоры и PDF в одном месте",
            "Календарь по каждому курсу",
            "Материалы, лекции и оплата без лишних шагов",
        ],
        "actions": [
            {
                "label": "Открыть мои курсы" if has_agreements else "Выбрать курс",
                "href": "#student-courses" if has_agreements else "/#courses",
                "variant": "primary",
            },
            {
                "label": "Договоры",
                "href": "#student-contracts",
                "variant": "secondary",
                "show": has_agreements,
            },
        ],
        "metrics": [
            {"label": "Курсов", "value": total_courses},
            {"label": "Подписано договоров", "value": signed_contracts},
            {"label": "Оплата", "value": "СБП" if payments_enabled else "Offline"},
            {"label": "Доступ", "value": "Активен" if has_agreements else "Ожидает"},
            {"label": "Ближайшие события", "value": upcoming_events},
            {"label": "Записи лекций", "value": lecture_records},
        ],
    }


def _build_account_overview(*, has_agreements: bool, payments_enabled: bool) -> Dict[str, Any]:
    return {
        "kicker": "Overview",
        "title": "Что доступно сейчас",
        "cards": [
            {
                "kicker": "Маршрут",
                "title": "Курсы, договоры и прогресс в одном экране",
                "text": (
                    "Кабинет собран как рабочая панель: сначала обзор, затем детали по каждому "
                    "курсу, оплате, расписанию и материалам."
                ),
            },
            {
                "kicker": "Оплата",
                "title": "СБП включена" if payments_enabled else "Онлайн-оплата выключена",
                "text": (
                    "Можно оплачивать занятия прямо из карточки курса и продолжать незавершённый "
                    "платёж без повторного ввода данных."
                    if payments_enabled
                    else "Сейчас доступны договоры, календарь, записи и материалы. Онлайн-оплата появится позже."
                ),
            },
            {
                "kicker": "Материалы и встречи",
                "title": "Доступ открывается по активным курсам" if has_agreements else "Доступ откроется после записи",
                "text": (
                    "Внутри каждой карточки есть календарь месяца, модульные материалы, статусы "
                    "по договору и ссылки на записи лекций."
                ),
            },
        ],
    }


def build_account_content(
    agreements: List[Dict[str, Any]],
    *,
    user_display: str,
    payments_enabled: bool,
) -> Dict[str, Any]:
    schedule_payload = build_account_schedule_payload(agreements)
    lectures_payload = build_account_lectures_payload(agreements)
    schedule = schedule_payload["items"]
    lectures = lectures_payload["items"]
    total_courses = len(agreements)
    signed_contracts = sum(1 for item in agreements if item.get("contract_status_key") == "signed")
    stats = {
        "total_courses": total_courses,
        "signed_contracts": signed_contracts,
        "upcoming_events": len(schedule),
        "lecture_records": len(lectures),
    }
    sections = {
        "schedule": {
            **ACCOUNT_SECTION_META["schedule"],
            "api": ACCOUNT_API_ENDPOINTS["schedule"],
            "items": schedule,
        },
        "lectures": {
            **ACCOUNT_SECTION_META["lectures"],
            "api": ACCOUNT_API_ENDPOINTS["lectures"],
            "items": lectures,
        },
    }
    return {
        "schedule": schedule,
        "lectures": lectures,
        "schedule_payload": schedule_payload,
        "lectures_payload": lectures_payload,
        "api": dict(ACCOUNT_API_ENDPOINTS),
        "hero": _build_account_hero(
            user_display=user_display,
            has_agreements=bool(agreements),
            payments_enabled=payments_enabled,
            total_courses=stats["total_courses"],
            signed_contracts=stats["signed_contracts"],
            upcoming_events=stats["upcoming_events"],
            lecture_records=stats["lecture_records"],
        ),
        "overview": _build_account_overview(
            has_agreements=bool(agreements),
            payments_enabled=payments_enabled,
        ),
        "sections": sections,
        "stats": stats,
    }


def build_account_schedule_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_account_schedule_items(agreements)


def build_account_lectures_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_account_lecture_items(agreements)


def build_account_mock_content(
    agreements: List[Dict[str, Any]],
    *,
    user_display: str,
    payments_enabled: bool,
) -> Dict[str, Any]:
    return build_account_content(
        agreements,
        user_display=user_display,
        payments_enabled=payments_enabled,
    )
