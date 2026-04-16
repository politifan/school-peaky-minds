import re
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from core import (
    AGREEMENTS_DIR,
    HOMEWORK_ITEMS_FILE,
    LECTURE_DOCUMENTS_DIR,
    LECTURE_RECORDS_FILE,
    TEACHERS_FILE,
    build_month_calendar,
    lesson_entry_sessions,
    lesson_sessions_entry,
    load_json,
    month_key,
    moscow_now,
    save_json,
    select_lesson_session_index,
)


ACCOUNT_API_ENDPOINTS = {
    "schedule": "/api/account/schedule",
    "homework": "/api/account/homework",
    "lectures": "/api/account/lectures",
    "teachers": "/api/account/teachers",
}

ACCOUNT_SECTION_META = {
    "schedule": {
        "kicker": "Schedule",
        "title": "Расписание",
        "empty_title": "Расписание пока пустое",
        "empty_text": "Когда курс и занятия будут назначены, ближайшие события появятся здесь и в API кабинета.",
    },
    "homework": {
        "kicker": "Homework",
        "title": "Домашние задания",
        "empty_title": "Домашних заданий пока нет",
        "empty_text": "Как только преподаватель выдаст задание по модулю, оно появится здесь вместе с дедлайном и материалами.",
    },
    "lectures": {
        "kicker": "Lectures",
        "title": "Мои лекции",
        "empty_title": "Записей лекций пока нет",
        "empty_text": "Когда преподаватель загрузит первую запись, она появится здесь автоматически.",
    },
    "teachers": {
        "kicker": "Teachers",
        "title": "Мои преподаватели",
        "empty_title": "Преподаватель пока не назначен",
        "empty_text": "Когда куратор назначит преподавателя на календарные даты, его карточка появится здесь.",
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

HOMEWORK_STATUS_META = {
    "assigned": {"label": "Назначено", "accent": "violet"},
    "submitted": {"label": "Отправлено", "accent": "sky"},
    "review": {"label": "На проверке", "accent": "mint"},
    "revision": {"label": "Нужна доработка", "accent": "warm"},
    "done": {"label": "Принято", "accent": "good"},
}

TEACHER_STATUS_META = {
    "active": "Активен",
    "paused": "Пауза",
}

TEACHER_ACCENTS = {"violet", "mint", "sky", "sunset"}

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


def _format_date_label(value: str) -> str:
    date_value = _parse_date_key(value)
    if not date_value:
        return str(value or "")
    return date_value.strftime("%d.%m.%Y")


def _format_datetime_label(value: datetime) -> str:
    return value.strftime("%d.%m · %H:%M")


def _format_schedule_label(value: date, time_value: str) -> str:
    if time_value:
        return f"{value.strftime('%d.%m')} · {time_value}"
    return f"{value.strftime('%d.%m')} · время уточняется"


def _format_iso_label(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return value
    return dt.strftime("%d.%m.%Y")


def _split_csv(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _normalize_page(value: Any) -> int:
    try:
        page = int(value)
    except Exception:
        page = 1
    return max(page, 1)


def _normalize_per_page(value: Any, default: int = 6, max_value: int = 24) -> int:
    try:
        size = int(value)
    except Exception:
        size = default
    return max(1, min(size, max_value))


def _paginate_items(items: List[Dict[str, Any]], *, page: int, per_page: int) -> Dict[str, Any]:
    total = len(items)
    pages = max((total + per_page - 1) // per_page, 1)
    current = min(max(page, 1), pages)
    start = (current - 1) * per_page
    end = start + per_page
    return {
        "items": items[start:end],
        "page": current,
        "per_page": per_page,
        "pages": pages,
        "total": total,
        "has_prev": current > 1,
        "has_next": current < pages,
        "prev_page": current - 1 if current > 1 else 1,
        "next_page": current + 1 if current < pages else pages,
    }


def _initials(value: str) -> str:
    parts = [item for item in re.split(r"\s+", str(value or "").strip()) if item]
    if not parts:
        return "PM"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][:1] + parts[1][:1]).upper()


def _teacher_contact_url(item: Dict[str, Any]) -> str:
    telegram = str(item.get("telegram") or "").strip()
    email = str(item.get("email") or "").strip()
    if telegram:
        handle = telegram[1:] if telegram.startswith("@") else telegram
        return f"https://t.me/{handle}"
    if email:
        return f"mailto:{email}"
    return ""


def _is_valid_url(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    if raw.startswith("/documents/lectures/"):
        return True
    parsed = urlparse(raw)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _lecture_public_url(file_name: str) -> str:
    return f"/documents/lectures/{file_name}"


def _extract_lesson_status_map(item: Dict[str, Any]) -> Dict[str, Any]:
    lesson_calendar_map = item.get("lesson_calendar_map")
    if isinstance(lesson_calendar_map, dict):
        return lesson_calendar_map
    lesson_calendar = item.get("lesson_calendar")
    if isinstance(lesson_calendar, dict):
        return lesson_calendar
    return {}


def _agreement_lookup(agreements: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup = {}
    for item in agreements:
        agreement_file = str(item.get("agreement_file") or item.get("_file") or item.get("file") or "").strip()
        if agreement_file:
            lookup[agreement_file] = item
    return lookup


def _normalize_teacher_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    teacher_id = str(item.get("id") or "").strip() or f"teacher-{secrets.token_hex(6)}"
    name = str(item.get("name") or "").strip()
    if not name:
        return None
    status = str(item.get("status") or "active").strip()
    if status not in TEACHER_STATUS_META:
        status = "active"
    accent = str(item.get("accent") or "violet").strip()
    if accent not in TEACHER_ACCENTS:
        accent = "violet"
    created_at = str(item.get("created_at") or "").strip() or moscow_now().isoformat()
    updated_at = str(item.get("updated_at") or "").strip() or created_at
    expertise = _split_csv(item.get("expertise"))
    return {
        "id": teacher_id,
        "name": name,
        "initials": _initials(name),
        "role": str(item.get("role") or "").strip() or "Преподаватель",
        "bio": str(item.get("bio") or "").strip(),
        "telegram": str(item.get("telegram") or "").strip(),
        "email": str(item.get("email") or "").strip(),
        "speciality": str(item.get("speciality") or "").strip(),
        "expertise": expertise,
        "status": status,
        "status_label": TEACHER_STATUS_META[status],
        "accent": accent,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def load_teachers() -> List[Dict[str, Any]]:
    data = load_json(TEACHERS_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        raw_items = []
    items = []
    seen = set()
    for raw_item in raw_items:
        normalized = _normalize_teacher_record(raw_item)
        if not normalized or normalized["id"] in seen:
            continue
        seen.add(normalized["id"])
        items.append(normalized)
    items.sort(key=lambda item: (item["status"] != "active", item["name"].lower()))
    return items


def save_teachers(items: List[Dict[str, Any]]) -> None:
    save_json(TEACHERS_FILE, {"items": items})


def list_teachers(*, status: str = "") -> List[Dict[str, Any]]:
    items = load_teachers()
    status_key = str(status or "").strip()
    if not status_key:
        return items
    return [item for item in items if item["status"] == status_key]


def get_teacher_map() -> Dict[str, Dict[str, Any]]:
    return {item["id"]: item for item in load_teachers()}


def get_teacher_record(teacher_id: str) -> Optional[Dict[str, Any]]:
    key = str(teacher_id or "").strip()
    if not key:
        return None
    for item in load_teachers():
        if item["id"] == key:
            return item
    return None


def upsert_teacher_record(
    *,
    name: str,
    role: str = "",
    bio: str = "",
    speciality: str = "",
    telegram: str = "",
    email: str = "",
    expertise: Any = None,
    status: str = "active",
    accent: str = "violet",
    teacher_id: str = "",
) -> Dict[str, Any]:
    items = load_teachers()
    existing = None
    record_id = str(teacher_id or "").strip()
    if record_id:
        for item in items:
            if item["id"] == record_id:
                existing = item
                break

    now_iso = moscow_now().isoformat()
    payload = _normalize_teacher_record(
        {
            "id": record_id or f"teacher-{secrets.token_hex(6)}",
            "name": name,
            "role": role or (existing.get("role") if existing else ""),
            "bio": bio,
            "speciality": speciality,
            "telegram": telegram,
            "email": email,
            "expertise": expertise,
            "status": status or (existing.get("status") if existing else "active"),
            "accent": accent or (existing.get("accent") if existing else "violet"),
            "created_at": existing.get("created_at") if existing else now_iso,
            "updated_at": now_iso,
        }
    )
    if not payload:
        raise ValueError("Укажите имя преподавателя")

    updated = [item for item in items if item["id"] != payload["id"]]
    updated.append(payload)
    save_teachers(updated)
    return payload


def delete_teacher_record(teacher_id: str) -> bool:
    key = str(teacher_id or "").strip()
    if not key:
        return False

    items = load_teachers()
    updated = [item for item in items if item["id"] != key]
    if len(updated) == len(items):
        return False
    save_teachers(updated)

    for path in AGREEMENTS_DIR.glob("agreement_*.json"):
        data = load_json(path, {})
        if not isinstance(data, dict):
            continue
        calendar_map = data.get("lesson_calendar") if isinstance(data.get("lesson_calendar"), dict) else {}
        changed = False
        for date_key, entry in list(calendar_map.items()):
            sessions = lesson_entry_sessions(entry)
            if not sessions:
                continue
            updated_sessions = []
            for session in sessions:
                if str(session.get("teacher_id") or "").strip() == key:
                    session = {**session, "teacher_id": ""}
                    changed = True
                updated_sessions.append(session)
            compact_entry = lesson_sessions_entry(updated_sessions)
            if compact_entry:
                calendar_map[date_key] = compact_entry
            else:
                calendar_map.pop(date_key, None)
        if changed:
            data["lesson_calendar"] = calendar_map
            save_json(path, data)

    homework_items = load_homework_items()
    homework_changed = False
    for item in homework_items:
        if item.get("teacher_id") == key:
            item["teacher_id"] = ""
            homework_changed = True
    if homework_changed:
        save_homework_items(homework_items)
    return True


def assign_teacher_to_lesson(
    *,
    file_name: str,
    date_raw: str,
    teacher_id: str = "",
    time_raw: str = "",
    status_raw: str = "",
) -> Dict[str, Any]:
    file_key = str(file_name or "").strip()
    if not file_key:
        raise ValueError("Не указан договор")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(date_raw or "").strip()):
        raise ValueError("Некорректная дата")

    teacher_key = str(teacher_id or "").strip()
    if teacher_key and not get_teacher_record(teacher_key):
        raise ValueError("Преподаватель не найден")

    time_value = _normalize_time_value(time_raw)
    if time_raw and not time_value:
        raise ValueError("Некорректное время")

    path = AGREEMENTS_DIR / file_key
    if not path.exists():
        raise ValueError("Договор не найден")
    data = load_json(path, {})
    if not isinstance(data, dict):
        raise ValueError("Договор не найден")

    calendar_map = data.get("lesson_calendar") if isinstance(data.get("lesson_calendar"), dict) else {}
    sessions = lesson_entry_sessions(calendar_map.get(date_raw))

    if status_raw:
        if status_raw not in {"proposed", "approved", "missed", "excused", "clear"}:
            raise ValueError("Некорректный статус")
        if status_raw == "clear":
            if not time_value:
                calendar_map.pop(date_raw, None)
                data["lesson_calendar"] = calendar_map
                save_json(path, data)
                return {"date": date_raw, "teacher_id": "", "status": "", "time": ""}
            index = select_lesson_session_index(sessions, time_value)
            if index is None:
                raise ValueError("Занятие с таким временем не найдено")
            sessions.pop(index)
        else:
            index = select_lesson_session_index(sessions, time_value)
            if index is None:
                session = {
                    "status": status_raw,
                    "time": time_value,
                    "teacher_id": teacher_key,
                }
                sessions.append(session)
            else:
                if status_raw:
                    sessions[index]["status"] = status_raw
                if time_raw:
                    sessions[index]["time"] = time_value
                if teacher_key:
                    sessions[index]["teacher_id"] = teacher_key
                elif "teacher_id" in sessions[index]:
                    sessions[index]["teacher_id"] = ""
    else:
        index = select_lesson_session_index(sessions, time_value)
        if index is None:
            sessions.append(
                {
                    "status": "proposed",
                    "time": time_value,
                    "teacher_id": teacher_key,
                }
            )
        else:
            if teacher_key:
                sessions[index]["teacher_id"] = teacher_key
            else:
                sessions[index]["teacher_id"] = ""
            if time_raw:
                sessions[index]["time"] = time_value

    for session in sessions:
        if not session.get("status"):
            session["status"] = "proposed"

    compact_entry = lesson_sessions_entry(sessions)
    if compact_entry:
        calendar_map[date_raw] = compact_entry
    else:
        calendar_map.pop(date_raw, None)
    data["lesson_calendar"] = calendar_map
    save_json(path, data)
    primary = sessions[0] if sessions else {}
    return {
        "date": date_raw,
        "teacher_id": teacher_key,
        "status": primary.get("status") or "",
        "time": primary.get("time") or "",
        "session_count": len(sessions),
    }


def _normalize_homework_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    homework_id = str(item.get("id") or "").strip() or f"hw-{secrets.token_hex(8)}"
    agreement_file = str(item.get("agreement_file") or "").strip()
    title = str(item.get("title") or "").strip()
    if not agreement_file or not title:
        return None
    status = str(item.get("status") or "assigned").strip()
    if status not in HOMEWORK_STATUS_META:
        status = "assigned"
    due_at = str(item.get("due_at") or "").strip()
    if due_at and not _parse_date_key(due_at):
        due_at = ""
    resource_url = str(item.get("resource_url") or "").strip()
    answer_url = str(item.get("answer_url") or "").strip()
    if resource_url and not _is_valid_url(resource_url):
        resource_url = ""
    if answer_url and not _is_valid_url(answer_url):
        answer_url = ""
    created_at = str(item.get("created_at") or "").strip() or moscow_now().isoformat()
    updated_at = str(item.get("updated_at") or "").strip() or created_at
    teacher_id = str(item.get("teacher_id") or "").strip()
    return {
        "id": homework_id,
        "agreement_file": agreement_file,
        "course": str(item.get("course") or "").strip(),
        "title": title,
        "module": str(item.get("module") or "").strip() or "Текущий модуль",
        "description": str(item.get("description") or "").strip(),
        "status": status,
        "status_label": HOMEWORK_STATUS_META[status]["label"],
        "status_accent": HOMEWORK_STATUS_META[status]["accent"],
        "due_at": due_at,
        "due_label": _format_date_label(due_at) if due_at else "Без дедлайна",
        "resource_url": resource_url,
        "answer_url": answer_url,
        "teacher_id": teacher_id,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def load_homework_items() -> List[Dict[str, Any]]:
    data = load_json(HOMEWORK_ITEMS_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        raw_items = []
    items = []
    seen = set()
    for raw_item in raw_items:
        normalized = _normalize_homework_record(raw_item)
        if not normalized or normalized["id"] in seen:
            continue
        seen.add(normalized["id"])
        items.append(normalized)
    items.sort(key=lambda item: (item["due_at"] or "9999-99-99", item["title"].lower()))
    return items


def save_homework_items(items: List[Dict[str, Any]]) -> None:
    save_json(HOMEWORK_ITEMS_FILE, {"items": items})


def list_homework_items(
    *,
    agreement_file: str = "",
    agreement_files: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    single = str(agreement_file or "").strip()
    allowed = {single} if single else set()
    if agreement_files:
        allowed.update(str(item or "").strip() for item in agreement_files if str(item or "").strip())
    items = load_homework_items()
    if not allowed:
        return items
    return [item for item in items if item["agreement_file"] in allowed]


def upsert_homework_item(
    *,
    agreement_file: str,
    course: str,
    title: str,
    module: str = "",
    description: str = "",
    status: str = "assigned",
    due_at: str = "",
    teacher_id: str = "",
    resource_url: str = "",
    answer_url: str = "",
    homework_id: str = "",
) -> Dict[str, Any]:
    file_key = str(agreement_file or "").strip()
    title_value = str(title or "").strip()
    if not file_key:
        raise ValueError("Не указан договор")
    if not title_value:
        raise ValueError("Укажите название домашнего задания")
    if teacher_id and not get_teacher_record(teacher_id):
        raise ValueError("Преподаватель не найден")

    items = load_homework_items()
    existing = None
    record_id = str(homework_id or "").strip()
    if record_id:
        for item in items:
            if item["id"] == record_id:
                existing = item
                break

    now_iso = moscow_now().isoformat()
    payload = _normalize_homework_record(
        {
            "id": record_id or f"hw-{secrets.token_hex(8)}",
            "agreement_file": file_key,
            "course": str(course or "").strip() or (existing.get("course") if existing else ""),
            "title": title_value,
            "module": module,
            "description": description,
            "status": status or (existing.get("status") if existing else "assigned"),
            "due_at": due_at or (existing.get("due_at") if existing else ""),
            "teacher_id": teacher_id,
            "resource_url": resource_url,
            "answer_url": answer_url,
            "created_at": existing.get("created_at") if existing else now_iso,
            "updated_at": now_iso,
        }
    )
    if not payload:
        raise ValueError("Не удалось сохранить домашнее задание")

    updated = [item for item in items if item["id"] != payload["id"]]
    updated.append(payload)
    save_homework_items(updated)
    return payload


def delete_homework_item(homework_id: str) -> bool:
    key = str(homework_id or "").strip()
    if not key:
        return False
    items = load_homework_items()
    updated = [item for item in items if item["id"] != key]
    if len(updated) == len(items):
        return False
    save_homework_items(updated)
    return True


def _normalize_lecture_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    record_id = str(item.get("id") or "").strip() or f"lecture-{secrets.token_hex(8)}"
    agreement_file = str(item.get("agreement_file") or "").strip()
    title = str(item.get("title") or "").strip()
    topic = str(item.get("topic") or "").strip()
    if not agreement_file or not (title or topic):
        return None
    file_name = str(item.get("file_name") or "").strip()
    url = str(item.get("url") or "").strip()
    if file_name and not url:
        url = _lecture_public_url(file_name)
    if not _is_valid_url(url):
        return None
    published_at = str(item.get("published_at") or "").strip() or moscow_now().isoformat()
    created_at = str(item.get("created_at") or "").strip() or published_at
    updated_at = str(item.get("updated_at") or "").strip() or created_at
    source_type = str(item.get("source_type") or "").strip()
    if source_type not in {"upload", "link"}:
        source_type = "upload" if file_name else "link"
    display_title = title or topic or "Лекция без названия"
    return {
        "id": record_id,
        "agreement_file": agreement_file,
        "course": str(item.get("course") or "").strip(),
        "title": title,
        "topic": topic,
        "display_title": display_title,
        "description": str(item.get("description") or "").strip(),
        "duration_label": str(item.get("duration_label") or "").strip() or "60 минут",
        "teacher": str(item.get("teacher") or "").strip() or "Преподаватель Peaky Minds",
        "published_at": published_at,
        "published_label": _format_iso_label(published_at),
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
    items = []
    seen = set()
    for raw_item in raw_items:
        normalized = _normalize_lecture_record(raw_item)
        if not normalized or normalized["id"] in seen:
            continue
        seen.add(normalized["id"])
        items.append(normalized)
    items.sort(key=lambda item: (item["published_at"], item["updated_at"], item["id"]), reverse=True)
    return items


def save_lecture_records(items: List[Dict[str, Any]]) -> None:
    save_json(LECTURE_RECORDS_FILE, {"items": items})


def get_lecture_record(record_id: str) -> Optional[Dict[str, Any]]:
    key = str(record_id or "").strip()
    if not key:
        return None
    for item in load_lecture_records():
        if item["id"] == key:
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


def _filter_lecture_records(
    items: List[Dict[str, Any]],
    *,
    q: str = "",
    source_type: str = "",
    course: str = "",
) -> List[Dict[str, Any]]:
    query = str(q or "").strip().lower()
    source_key = str(source_type or "").strip()
    course_key = str(course or "").strip()

    def matches(item: Dict[str, Any]) -> bool:
        if source_key and item.get("source_type") != source_key:
            return False
        if course_key and item.get("course") != course_key:
            return False
        if not query:
            return True
        haystack = " ".join(
            [
                str(item.get("display_title") or ""),
                str(item.get("title") or ""),
                str(item.get("topic") or ""),
                str(item.get("description") or ""),
                str(item.get("teacher") or ""),
                str(item.get("course") or ""),
                str(item.get("student_name") or ""),
            ]
        ).lower()
        return query in haystack

    return [item for item in items if matches(item)]


def build_lecture_registry_payload(
    agreements: List[Dict[str, Any]],
    *,
    agreement_file: str = "",
    agreement_files: Optional[List[str]] = None,
    q: str = "",
    source_type: str = "",
    course: str = "",
    page: int = 1,
    per_page: int = 6,
) -> Dict[str, Any]:
    agreement_map = _agreement_lookup(agreements)
    requested_files = agreement_files or list(agreement_map.keys())
    if agreement_file:
        requested_files = [agreement_file]

    records = list_lecture_records(agreement_files=requested_files)
    enriched = []
    for record in records:
        agreement = agreement_map.get(record["agreement_file"], {})
        enriched.append(
            {
                **record,
                "course": record.get("course") or str(agreement.get("course") or "").strip() or "Курс",
                "student_name": str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Без имени",
            }
        )

    source_options = sorted({item["source_type"] for item in enriched if item.get("source_type")})
    course_options = sorted({item["course"] for item in enriched if item.get("course")})
    filtered = _filter_lecture_records(enriched, q=q, source_type=source_type, course=course)
    pagination = _paginate_items(
        filtered,
        page=_normalize_page(page),
        per_page=_normalize_per_page(per_page),
    )
    return {
        **pagination,
        "total_all": len(enriched),
        "source_options": source_options,
        "course_options": course_options,
        "filters": {
            "q": str(q or "").strip(),
            "source": source_type,
            "course": course,
        },
    }


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
    topic: str = "",
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
    topic_value = str(topic or "").strip()
    if not agreement_key:
        raise ValueError("Не указан договор для записи")
    if not (title_value or topic_value):
        raise ValueError("Укажите название записи или тему")

    items = load_lecture_records()
    existing = None
    key = str(record_id or "").strip()
    if key:
        for item in items:
            if item["id"] == key:
                existing = item
                break

    upload_name = str(getattr(upload, "filename", "") or "").strip()
    saved_file_name = ""
    source_type = "link"
    url_value = str(url or "").strip()

    if upload_name:
        saved_file_name, url_value = save_lecture_upload(upload)
        source_type = "upload"
    elif url_value:
        if not _is_valid_url(url_value):
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
            "id": key or f"lecture-{secrets.token_hex(8)}",
            "agreement_file": agreement_key,
            "course": str(course or "").strip() or (existing.get("course") if existing else ""),
            "title": title_value,
            "topic": topic_value or (existing.get("topic") if existing and not title_value else topic_value),
            "description": description,
            "duration_label": duration_label,
            "teacher": teacher,
            "published_at": published_at or (existing.get("published_at") if existing else now_iso),
            "source_type": source_type,
            "url": url_value,
            "file_name": saved_file_name,
            "created_at": existing.get("created_at") if existing else now_iso,
            "updated_at": now_iso,
        }
    )
    if not payload:
        raise ValueError("Не удалось сохранить запись")

    updated = [item for item in items if item["id"] != payload["id"]]
    updated.append(payload)
    save_lecture_records(updated)
    return payload


def delete_lecture_record(record_id: str) -> bool:
    key = str(record_id or "").strip()
    if not key:
        return False
    items = load_lecture_records()
    removed = None
    updated = []
    for item in items:
        if item["id"] == key:
            removed = item
            continue
        updated.append(item)
    if not removed:
        return False
    if removed.get("file_name"):
        delete_lecture_file(removed.get("file_name") or "")
    save_lecture_records(updated)
    return True


def build_teacher_assignment_rows(
    agreements: List[Dict[str, Any]],
    *,
    month: str = "",
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    month_value = _normalize_month(month)
    rows = []
    for agreement in agreements:
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or "").strip()
        course = str(agreement.get("course") or "").strip() or "Курс"
        student_name = str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Без имени"
        for date_key, entry in _extract_lesson_status_map(agreement).items():
            if not date_key.startswith(month_value):
                continue
            sessions = lesson_entry_sessions(entry)
            for session_index, session in enumerate(sessions):
                status = str(session.get("status") or "").strip() or "proposed"
                time_value = _normalize_time_value(session.get("time"))
                teacher_id = str(session.get("teacher_id") or "").strip()
                teacher = teacher_map.get(teacher_id)
                rows.append(
                    {
                        "agreement_file": agreement_file,
                        "student_name": student_name,
                        "course": course,
                        "date": date_key,
                        "date_label": _format_date_label(date_key),
                        "time": time_value or "—",
                        "status": status,
                        "status_label": SCHEDULE_STATUS_META.get(status, SCHEDULE_STATUS_META["proposed"])["label"],
                        "teacher_id": teacher_id,
                        "teacher_name": teacher.get("name") if teacher else "Не назначен",
                        "session_key": f"{date_key}:{time_value or session_index}",
                    }
                )
    rows.sort(key=lambda item: (item["date"], item["time"], item["course"], item["student_name"]))
    return rows


def build_teacher_overview_items(
    agreements: List[Dict[str, Any]],
    *,
    month: str = "",
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    rows = build_teacher_assignment_rows(agreements, month=month, teacher_map=teacher_map)
    today = moscow_now().date()
    stats_map: Dict[str, Dict[str, Any]] = {
        teacher_id: {
            "assignments_count": 0,
            "students": set(),
            "courses": set(),
            "upcoming_count": 0,
            "next_date": "",
        }
        for teacher_id in teacher_map
    }
    for row in rows:
        teacher_id = row["teacher_id"]
        if not teacher_id or teacher_id not in stats_map:
            continue
        stats = stats_map[teacher_id]
        stats["assignments_count"] += 1
        stats["students"].add(row["student_name"])
        stats["courses"].add(row["course"])
        row_date = _parse_date_key(row["date"])
        if row_date and row_date >= today:
            stats["upcoming_count"] += 1
            if not stats["next_date"] or row["date"] < stats["next_date"]:
                stats["next_date"] = row["date"]

    items = []
    for teacher_id, teacher in teacher_map.items():
        stats = stats_map.get(teacher_id, {})
        items.append(
            {
                **teacher,
                "assignments_count": stats.get("assignments_count", 0),
                "students_count": len(stats.get("students", set())),
                "courses_count": len(stats.get("courses", set())),
                "upcoming_count": stats.get("upcoming_count", 0),
                "next_date_label": _format_date_label(stats["next_date"]) if stats.get("next_date") else "Нет даты",
                "contact_url": _teacher_contact_url(teacher),
            }
        )
    items.sort(key=lambda item: (item["status"] != "active", -item["assignments_count"], item["name"].lower()))
    return items


def build_homework_admin_items(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    agreement_map = _agreement_lookup(agreements)
    items = []
    for item in load_homework_items():
        agreement = agreement_map.get(item["agreement_file"], {})
        teacher = teacher_map.get(item.get("teacher_id") or "")
        items.append(
            {
                **item,
                "student_name": str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Без имени",
                "course": item.get("course") or str(agreement.get("course") or "").strip() or "Курс",
                "teacher_name": teacher.get("name") if teacher else "Без преподавателя",
            }
        )
    items.sort(key=lambda item: (item["due_at"] or "9999-99-99", item["title"].lower()))
    return items


def build_account_schedule_items(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    today = moscow_now().date()
    items: List[Dict[str, Any]] = []
    for agreement in agreements:
        course = str(agreement.get("course") or "").strip() or "Курс"
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or "").strip()
        for date_key, entry in _extract_lesson_status_map(agreement).items():
            date_value = _parse_date_key(date_key)
            if not date_value or date_value < today:
                continue
            sessions = lesson_entry_sessions(entry)
            for session_index, session in enumerate(sessions):
                status = str(session.get("status") or "").strip()
                time_value = _normalize_time_value(session.get("time"))
                teacher_id = str(session.get("teacher_id") or "").strip()
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
                teacher = teacher_map.get(teacher_id)
                teacher_name = teacher.get("name") if teacher else "Преподаватель будет назначен"
                meta = SCHEDULE_STATUS_META[status]
                items.append(
                    {
                        "id": f"{agreement_file}:{date_key}:{time_value or session_index}",
                        "agreement_file": agreement_file,
                        "course": course,
                        "title": meta["event_title"],
                        "starts_at": starts_at.isoformat(),
                        "starts_label": _format_schedule_label(date_value, time_value),
                        "duration_label": "60 минут",
                        "channel": "Онлайн",
                        "teacher": teacher_name,
                        "teacher_id": teacher_id,
                        "kind": meta["kind"],
                        "status": status,
                        "_sort_key": starts_at.timestamp(),
                    }
                )
    items.sort(key=lambda item: (item["_sort_key"], item["course"], item["id"]))
    for item in items:
        item.pop("_sort_key", None)
    return items[:limit]


def build_account_schedule_payload(
    agreements: List[Dict[str, Any]],
    *,
    month: str = "",
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    teacher_map = teacher_map or get_teacher_map()
    month_value = _normalize_month(month)
    upcoming_items = build_account_schedule_items(agreements, teacher_map=teacher_map)
    next_by_agreement = {item["agreement_file"]: item for item in upcoming_items if item.get("agreement_file")}
    calendars = []
    for agreement in agreements:
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or "").strip()
        course = str(agreement.get("course") or "").strip() or "Курс"
        calendar_map = _extract_lesson_status_map(agreement)
        calendar_weeks = build_month_calendar(month_value, calendar_map)
        month_events = 0
        teacher_ids = set()
        for date_key, entry in calendar_map.items():
            if not date_key.startswith(month_value):
                continue
            for session in lesson_entry_sessions(entry):
                status = str(session.get("status") or "").strip()
                teacher_id = str(session.get("teacher_id") or "").strip()
                if status in SCHEDULE_STATUS_META:
                    month_events += 1
                if teacher_id:
                    teacher_ids.add(teacher_id)
        calendars.append(
            {
                "agreement_file": agreement_file,
                "course": course,
                "month": month_value,
                "calendar": calendar_weeks,
                "items_count": month_events,
                "teachers": [teacher_map[teacher_id]["name"] for teacher_id in teacher_ids if teacher_id in teacher_map],
                "next_event": next_by_agreement.get(agreement_file),
            }
        )
    return {
        "month": month_value,
        "legend": [{"key": key, "label": meta["label"]} for key, meta in SCHEDULE_STATUS_META.items()],
        "items": upcoming_items,
        "calendars": calendars,
        "total": len(upcoming_items),
    }


def build_account_homework_items(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    agreement_map = _agreement_lookup(agreements)
    items = []
    for item in list_homework_items(agreement_files=list(agreement_map.keys())):
        agreement = agreement_map.get(item["agreement_file"], {})
        teacher = teacher_map.get(item.get("teacher_id") or "")
        items.append(
            {
                **item,
                "course": item.get("course") or str(agreement.get("course") or "").strip() or "Курс",
                "teacher_name": teacher.get("name") if teacher else "Преподаватель будет назначен",
                "teacher_role": teacher.get("role") if teacher else "Куратор курса",
            }
        )
    items.sort(key=lambda item: (item["due_at"] or "9999-99-99", item["title"].lower()))
    return items


def build_account_homework_payload(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    items = build_account_homework_items(agreements, teacher_map=teacher_map)
    open_count = sum(1 for item in items if item["status"] in {"assigned", "submitted", "review", "revision"})
    return {
        "items": items,
        "total": len(items),
        "open_count": open_count,
        "status_meta": HOMEWORK_STATUS_META,
    }


def build_account_lecture_items(
    agreements: List[Dict[str, Any]],
    *,
    q: str = "",
    source_type: str = "",
    course: str = "",
    page: int = 1,
    per_page: int = 6,
) -> List[Dict[str, Any]]:
    payload = build_lecture_registry_payload(
        agreements,
        q=q,
        source_type=source_type,
        course=course,
        page=page,
        per_page=per_page,
    )
    return payload["items"]


def build_account_lectures_payload(
    agreements: List[Dict[str, Any]],
    *,
    q: str = "",
    source_type: str = "",
    course: str = "",
    page: int = 1,
    per_page: int = 6,
) -> Dict[str, Any]:
    payload = build_lecture_registry_payload(
        agreements,
        q=q,
        source_type=source_type,
        course=course,
        page=page,
        per_page=per_page,
    )
    items = payload["items"]
    return {
        **payload,
        "items": items,
        "storage": {
            "mode": "server-filesystem",
            "public_base_url": "/documents/lectures/",
        },
    }


def build_account_teacher_items(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
    homework_items: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    teacher_map = teacher_map or get_teacher_map()
    homework_items = homework_items if homework_items is not None else build_account_homework_items(agreements, teacher_map=teacher_map)
    assignment_rows = build_teacher_assignment_rows(agreements, teacher_map=teacher_map)
    rows_by_teacher: Dict[str, List[Dict[str, Any]]] = {}
    for row in assignment_rows:
        teacher_id = row.get("teacher_id") or ""
        if teacher_id:
            rows_by_teacher.setdefault(teacher_id, []).append(row)
    homework_by_teacher: Dict[str, List[Dict[str, Any]]] = {}
    for item in homework_items:
        teacher_id = item.get("teacher_id") or ""
        if teacher_id:
            homework_by_teacher.setdefault(teacher_id, []).append(item)

    items = []
    for teacher_id in sorted(set(rows_by_teacher) | set(homework_by_teacher)):
        teacher = teacher_map.get(teacher_id)
        if not teacher:
            continue
        assignments = rows_by_teacher.get(teacher_id, [])
        homework = homework_by_teacher.get(teacher_id, [])
        next_date = ""
        for row in assignments:
            if not next_date or row["date"] < next_date:
                next_date = row["date"]
        items.append(
            {
                **teacher,
                "contact_url": _teacher_contact_url(teacher),
                "upcoming_count": len(assignments),
                "homework_count": len(homework),
                "courses": sorted({row["course"] for row in assignments} | {item["course"] for item in homework}),
                "next_date_label": _format_date_label(next_date) if next_date else "Дата появится позже",
            }
        )
    items.sort(key=lambda item: (-item["upcoming_count"], -item["homework_count"], item["name"].lower()))
    return items


def build_account_teachers_payload(
    agreements: List[Dict[str, Any]],
    *,
    teacher_map: Optional[Dict[str, Dict[str, Any]]] = None,
    homework_items: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    items = build_account_teacher_items(agreements, teacher_map=teacher_map, homework_items=homework_items)
    return {
        "items": items,
        "total": len(items),
    }


def build_account_settings_payload(user: Optional[Dict[str, Any]], api_map: Dict[str, str]) -> Dict[str, Any]:
    user = user or {}
    provider_map = {
        "google": "Google",
        "github": "GitHub",
        "email": "Email-код",
        "vk": "VK",
        "telegram": "Telegram",
    }
    provider = provider_map.get(str(user.get("provider") or "").strip(), "Не указан")
    return {
        "profile_cards": [
            {
                "label": "Имя",
                "value": str(user.get("name") or "").strip() or "Не заполнено",
            },
            {
                "label": "Email",
                "value": str(user.get("email") or "").strip() or "Не привязан",
            },
            {
                "label": "Способ входа",
                "value": provider,
            },
        ],
        "utility_cards": [
            {
                "title": "API кабинета",
                "text": "Все ключевые разделы кабинета доступны и через JSON API. Это удобно для мобильного приложения и будущей LMS.",
                "tags": list(api_map.values()),
            },
            {
                "title": "Уведомления и поддержка",
                "text": "Следующим шагом сюда можно добавить push/email-уведомления о занятиях, дедлайнах и новых лекциях.",
                "tags": ["email", "calendar", "support"],
            },
        ],
    }


def _build_account_hero(
    *,
    user_display: str,
    has_agreements: bool,
    payments_enabled: bool,
    total_courses: int,
    signed_contracts: int,
    upcoming_events: int,
    homework_open: int,
    lecture_records: int,
    teachers_count: int,
) -> Dict[str, Any]:
    return {
        "kicker": "Student workspace",
        "title": "Личный кабинет",
        "user_display": user_display,
        "lead": "Здесь собраны договоры, прогресс по курсам, материалы, календарь, домашние задания и записи лекций.",
        "summary": [
            "Договоры и PDF в одном месте",
            "Календарь по каждому курсу",
            "Домашка, лекции и преподаватели без лишних шагов",
        ],
        "actions": [
            {
                "label": "Открыть мои курсы" if has_agreements else "Выбрать курс",
                "href": "/account/courses" if has_agreements else "/courses",
                "variant": "primary",
            },
            {
                "label": "Рабочее пространство",
                "href": "/account/calendar",
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
            {"label": "Активных ДЗ", "value": homework_open},
            {"label": "Записи лекций", "value": lecture_records},
            {"label": "Преподавателей", "value": teachers_count},
        ],
    }


def _build_account_focus(
    *,
    has_agreements: bool,
    payments_enabled: bool,
    total_courses: int,
    signed_contracts: int,
    upcoming_events: int,
    homework_open: int,
    lecture_records: int,
) -> Dict[str, Any]:
    if has_agreements:
        next_step = {
            "label": "Сейчас",
            "title": "Откройте workspace и проверьте ближайший фокус.",
            "text": "Календарь, ДЗ, лекции и преподаватели уже собраны внутри кабинета.",
            "action": "Открыть workspace",
            "href": "/account/calendar",
        }
    else:
        next_step = {
            "label": "Старт",
            "title": "Пока нет активного курса.",
            "text": "После записи здесь автоматически появятся календарь, материалы и оплата.",
            "action": "Выбрать курс",
            "href": "/courses",
        }

    if upcoming_events or homework_open:
        attention_title = f"{upcoming_events} событий и {homework_open} активных ДЗ"
        attention_text = "Начните с календаря и блока домашних заданий, чтобы сразу понять темп на ближайшие дни."
    else:
        attention_title = "Сейчас всё спокойно"
        attention_text = "Новых дедлайнов пока нет, значит можно спокойно догнать материалы и записи лекций."

    if payments_enabled:
        docs_title = f"{signed_contracts} договоров и онлайн-оплата в одном месте"
        docs_text = "Документы, PDF и оплата через СБП открываются из карточки курса."
    else:
        docs_title = f"{signed_contracts} договоров и материалы под рукой"
        docs_text = "Онлайн-оплата появится позже, но все учебные материалы и договоры уже останутся здесь."

    return {
        "title": "Короткий фокус на сейчас",
        "text": "Личный кабинет пересобран так, чтобы вы быстро видели свой следующий шаг, а не тонули в панелях.",
        "workspace_note": "Одна панель для календаря, ДЗ, лекций, преподавателей и настроек.",
        "cards": [
            next_step,
            {
                "label": "Внимание",
                "title": attention_title,
                "text": attention_text,
                "action": "Перейти к workspace",
                "href": "/account/homework" if homework_open else "/account/calendar",
            },
            {
                "label": "Документы и оплата",
                "title": docs_title,
                "text": docs_text,
                "action": "Открыть курсы",
                "href": "/account/documents" if total_courses else "/courses",
            },
            {
                "label": "Материалы",
                "title": f"{lecture_records} записей лекций и учебный контур под рукой",
                "text": "Записи, ссылки и преподаватели открываются из workspace без длинного поиска.",
                "action": "Открыть лекции",
                "href": "/account/lectures",
            },
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
                    "Кабинет собран как рабочая панель: обзор, календарь, домашние задания, лекции и настройки."
                ),
            },
            {
                "kicker": "Оплата",
                "title": "СБП включена" if payments_enabled else "Онлайн-оплата выключена",
                "text": (
                    "Можно оплачивать занятия прямо из карточки курса и продолжать незавершённый платёж без повторного ввода данных."
                    if payments_enabled
                    else "Сейчас доступны договоры, календарь, домашка, лекции и материалы. Онлайн-оплата появится позже."
                ),
            },
            {
                "kicker": "Контур обучения",
                "title": "Преподаватель, дедлайны и записи не теряются",
                "text": (
                    "Внутри каждой траектории видно, кто ведёт занятие, когда дедлайн по ДЗ и где лежит запись прошедшей лекции."
                    if has_agreements
                    else "После записи на курс здесь автоматически появятся преподаватели, домашние задания и записи."
                ),
            },
        ],
    }


def build_account_content(
    agreements: List[Dict[str, Any]],
    *,
    user_display: str,
    payments_enabled: bool,
    user: Optional[Dict[str, Any]] = None,
    lectures_query: str = "",
    lectures_source: str = "",
    lectures_course: str = "",
    lectures_page: int = 1,
    lectures_per_page: int = 6,
) -> Dict[str, Any]:
    teacher_map = get_teacher_map()
    schedule_payload = build_account_schedule_payload(agreements, teacher_map=teacher_map)
    homework_payload = build_account_homework_payload(agreements, teacher_map=teacher_map)
    lectures_payload = build_account_lectures_payload(
        agreements,
        q=lectures_query,
        source_type=lectures_source,
        course=lectures_course,
        page=lectures_page,
        per_page=lectures_per_page,
    )
    teachers_payload = build_account_teachers_payload(
        agreements,
        teacher_map=teacher_map,
        homework_items=homework_payload["items"],
    )
    api_map = dict(ACCOUNT_API_ENDPOINTS)
    settings_payload = build_account_settings_payload(user, api_map)

    schedule = schedule_payload["items"]
    homework = homework_payload["items"]
    lectures = lectures_payload["items"]
    teachers = teachers_payload["items"]
    total_courses = len(agreements)
    signed_contracts = sum(1 for item in agreements if item.get("contract_status_key") == "signed")
    stats = {
        "total_courses": total_courses,
        "signed_contracts": signed_contracts,
        "upcoming_events": len(schedule),
        "homework_open": homework_payload["open_count"],
        "lecture_records": lectures_payload["total_all"],
        "teachers_count": len(teachers),
    }
    sections = {
        "schedule": {
            **ACCOUNT_SECTION_META["schedule"],
            "api": api_map["schedule"],
            "items": schedule,
        },
        "homework": {
            **ACCOUNT_SECTION_META["homework"],
            "api": api_map["homework"],
            "items": homework,
        },
        "lectures": {
            **ACCOUNT_SECTION_META["lectures"],
            "api": api_map["lectures"],
            "items": lectures,
        },
        "teachers": {
            **ACCOUNT_SECTION_META["teachers"],
            "api": api_map["teachers"],
            "items": teachers,
        },
    }
    workspace_tabs = [
        {"key": "overview", "label": "Обзор", "count": ""},
        {"key": "calendar", "label": "Календарь", "count": str(len(schedule)) if schedule else ""},
        {"key": "homework", "label": "ДЗ", "count": str(homework_payload["open_count"]) if homework_payload["open_count"] else ""},
        {"key": "lectures", "label": "Лекции", "count": str(len(lectures)) if lectures else ""},
        {"key": "teachers", "label": "Преподаватели", "count": str(len(teachers)) if teachers else ""},
        {"key": "settings", "label": "Настройки", "count": ""},
    ]
    return {
        "schedule": schedule,
        "homework": homework,
        "lectures": lectures,
        "teachers": teachers,
        "schedule_payload": schedule_payload,
        "homework_payload": homework_payload,
        "lectures_payload": lectures_payload,
        "teachers_payload": teachers_payload,
        "settings_payload": settings_payload,
        "workspace_tabs": workspace_tabs,
        "api": api_map,
        "hero": _build_account_hero(
            user_display=user_display,
            has_agreements=bool(agreements),
            payments_enabled=payments_enabled,
            total_courses=stats["total_courses"],
            signed_contracts=stats["signed_contracts"],
            upcoming_events=stats["upcoming_events"],
            homework_open=stats["homework_open"],
            lecture_records=stats["lecture_records"],
            teachers_count=stats["teachers_count"],
        ),
        "focus": _build_account_focus(
            has_agreements=bool(agreements),
            payments_enabled=payments_enabled,
            total_courses=stats["total_courses"],
            signed_contracts=stats["signed_contracts"],
            upcoming_events=stats["upcoming_events"],
            homework_open=stats["homework_open"],
            lecture_records=stats["lecture_records"],
        ),
        "overview": _build_account_overview(
            has_agreements=bool(agreements),
            payments_enabled=payments_enabled,
        ),
        "sections": sections,
        "stats": stats,
    }


ACCOUNT_SHELL_PAGES = [
    {
        "key": "overview",
        "label": "Сегодня",
        "href": "/account",
        "count_key": "upcoming_events",
        "note": "Следующий шаг, ближайшие события и быстрый обзор обучения.",
    },
    {
        "key": "courses",
        "label": "Курсы",
        "href": "/account/courses",
        "count_key": "total_courses",
        "note": "Все траектории, прогресс по модулям и точки входа в материалы.",
    },
    {
        "key": "calendar",
        "label": "Календарь",
        "href": "/account/calendar",
        "count_key": "upcoming_events",
        "note": "Ближайшие занятия и помесячный ритм обучения без лишнего шума.",
    },
    {
        "key": "homework",
        "label": "ДЗ",
        "href": "/account/homework",
        "count_key": "homework_open",
        "note": "Активные домашние задания, дедлайны и нужные материалы в одном месте.",
    },
    {
        "key": "lectures",
        "label": "Лекции",
        "href": "/account/lectures",
        "count_key": "lecture_records",
        "note": "Записи занятий, фильтры и быстрый возврат к нужной теме.",
    },
    {
        "key": "teachers",
        "label": "Преподаватели",
        "href": "/account/teachers",
        "count_key": "teachers_count",
        "note": "Кто ведёт занятия, за что отвечает и как быстро связаться.",
    },
    {
        "key": "documents",
        "label": "Документы",
        "href": "/account/documents",
        "count_key": "signed_contracts",
        "note": "Договоры, PDF и сервисные детали без поиска по длинной странице.",
    },
    {
        "key": "profile",
        "label": "Профиль",
        "href": "/account/profile",
        "count_key": "",
        "note": "Способ входа, данные аккаунта и системные настройки кабинета.",
    },
]

ACCOUNT_WORKSPACE_ROUTE_MAP = {
    "overview": "/account",
    "calendar": "/account/calendar",
    "homework": "/account/homework",
    "lectures": "/account/lectures",
    "teachers": "/account/teachers",
    "settings": "/account/profile",
}

ACCOUNT_SHELL_PAGE_MAP = {item["key"]: item for item in ACCOUNT_SHELL_PAGES}


def build_account_shell(*, active_key: str, stats: Dict[str, Any]) -> Dict[str, Any]:
    active_page = ACCOUNT_SHELL_PAGE_MAP.get(active_key) or ACCOUNT_SHELL_PAGES[0]
    items: List[Dict[str, Any]] = []
    for page in ACCOUNT_SHELL_PAGES:
        count_value = stats.get(page.get("count_key", "")) if page.get("count_key") else None
        count = str(count_value) if isinstance(count_value, int) and count_value > 0 else ""
        items.append(
            {
                "key": page["key"],
                "label": page["label"],
                "href": page["href"],
                "count": count,
                "active": page["key"] == active_key,
            }
        )
    return {
        "eyebrow": "Student shell",
        "title": "Личный кабинет",
        "subtitle": active_page.get("note", ""),
        "active": active_key,
        "items": items,
        "scroll_target": "",
    }


def _account_page_meta(
    *,
    key: str,
    kicker: str,
    title: str,
    lead: str,
    metrics: Optional[List[Dict[str, Any]]] = None,
    actions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "key": key,
        "kicker": kicker,
        "title": title,
        "lead": lead,
        "metrics": metrics or [],
        "actions": actions or [],
        "seo_title": f"{title} - Личный кабинет Peaky Minds",
        "seo_description": lead,
    }


def build_account_overview_page(
    *,
    account_content: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    stats = account_content["stats"]
    schedule_preview = account_content["schedule_payload"]["items"][:3]
    homework_preview = account_content["homework_payload"]["items"][:3]
    lectures_preview = account_content["lectures_payload"]["items"][:3]
    page = _account_page_meta(
        key="overview",
        kicker="Today",
        title="Что важно сейчас",
        lead="Короткий обзор следующего шага, ближайших занятий, домашних заданий и лекций.",
        metrics=account_content["hero"]["metrics"][4:8],
        actions=[
            {"label": "Открыть календарь", "href": "/account/calendar", "variant": "secondary"},
            {"label": "Все курсы", "href": "/account/courses", "variant": "secondary", "show": bool(agreements)},
        ],
    )
    page.update(
        {
            "overview_cards": account_content["overview"]["cards"],
            "schedule_preview": schedule_preview,
            "homework_preview": homework_preview,
            "lectures_preview": lectures_preview,
            "schedule_total": stats["upcoming_events"],
            "homework_total": stats["homework_open"],
            "lecture_total": stats["lecture_records"],
        }
    )
    return {
        "template": "account_overview.html",
        "account_page": page,
        "account_shell": build_account_shell(active_key="overview", stats=stats),
    }


def build_account_courses_page(
    *,
    account_content: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_courses.html",
        "account_page": _account_page_meta(
            key="courses",
            kicker="Courses",
            title="Все ваши курсы",
            lead="Каждая траектория собрана как отдельный рабочий контур: прогресс, материалы, календарь и оплата рядом.",
            metrics=[
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Договоров", "value": stats["signed_contracts"]},
                {"label": "Занятий впереди", "value": stats["upcoming_events"]},
                {"label": "Лекций", "value": stats["lecture_records"]},
            ],
            actions=[
                {"label": "Календарь", "href": "/account/calendar", "variant": "secondary"},
                {"label": "Документы", "href": "/account/documents", "variant": "secondary", "show": bool(agreements)},
            ],
        ),
        "account_shell": build_account_shell(active_key="courses", stats=stats),
    }


def build_account_calendar_page(*, account_content: Dict[str, Any], agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_calendar.html",
        "account_page": _account_page_meta(
            key="calendar",
            kicker="Calendar",
            title="Календарь обучения",
            lead="Только ближайшие занятия, помесячная сетка и преподаватели по каждой активной траектории.",
            metrics=[
                {"label": "Событий", "value": stats["upcoming_events"]},
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Преподавателей", "value": stats["teachers_count"]},
                {"label": "ДЗ открыто", "value": stats["homework_open"]},
            ],
            actions=[
                {"label": "Домашние задания", "href": "/account/homework", "variant": "secondary"},
                {"label": "Лекции", "href": "/account/lectures", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="calendar", stats=stats),
    }


def build_account_homework_page(*, account_content: Dict[str, Any], agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_homework.html",
        "account_page": _account_page_meta(
            key="homework",
            kicker="Homework",
            title="Домашние задания",
            lead="Все активные задачи, дедлайны, материалы и быстрые переходы к ответам без лишнего поиска.",
            metrics=[
                {"label": "Активных ДЗ", "value": stats["homework_open"]},
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Лекций", "value": stats["lecture_records"]},
                {"label": "Событий", "value": stats["upcoming_events"]},
            ],
            actions=[
                {"label": "Открыть календарь", "href": "/account/calendar", "variant": "secondary"},
                {"label": "Открыть лекции", "href": "/account/lectures", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="homework", stats=stats),
    }


def build_account_lectures_page(*, account_content: Dict[str, Any], agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_lectures.html",
        "account_page": _account_page_meta(
            key="lectures",
            kicker="Lectures",
            title="Библиотека лекций",
            lead="Записи занятий, фильтры по курсам и быстрый возврат к нужной теме в отдельной спокойной странице.",
            metrics=[
                {"label": "Записей", "value": stats["lecture_records"]},
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Преподавателей", "value": stats["teachers_count"]},
                {"label": "ДЗ", "value": stats["homework_open"]},
            ],
            actions=[
                {"label": "Домашние задания", "href": "/account/homework", "variant": "secondary"},
                {"label": "Преподаватели", "href": "/account/teachers", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="lectures", stats=stats),
    }


def build_account_teachers_page(*, account_content: Dict[str, Any], agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_teachers.html",
        "account_page": _account_page_meta(
            key="teachers",
            kicker="Teachers",
            title="Ваши преподаватели",
            lead="Кто ведёт занятия, какие курсы закрывает и как быстро выйти на связь по делу.",
            metrics=[
                {"label": "Преподавателей", "value": stats["teachers_count"]},
                {"label": "Событий", "value": stats["upcoming_events"]},
                {"label": "ДЗ", "value": stats["homework_open"]},
                {"label": "Лекций", "value": stats["lecture_records"]},
            ],
            actions=[
                {"label": "Календарь", "href": "/account/calendar", "variant": "secondary"},
                {"label": "Лекции", "href": "/account/lectures", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="teachers", stats=stats),
    }


def build_account_documents_page(
    *,
    account_content: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    stats = account_content["stats"]
    return {
        "template": "account_documents.html",
        "account_page": _account_page_meta(
            key="documents",
            kicker="Documents",
            title="Договоры и PDF",
            lead="Все подписанные документы, PDF и сервисные карточки оплаты собраны отдельно от учебного контента.",
            metrics=[
                {"label": "Договоров", "value": stats["signed_contracts"]},
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Оплата", "value": "СБП" if account_content["hero"]["metrics"][2]["value"] == "СБП" else "Offline"},
                {"label": "Доступ", "value": account_content["hero"]["metrics"][3]["value"]},
            ],
            actions=[
                {"label": "Все курсы", "href": "/account/courses", "variant": "secondary"},
                {"label": "Профиль", "href": "/account/profile", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="documents", stats=stats),
    }


def build_account_profile_page(*, account_content: Dict[str, Any], agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = account_content["stats"]
    provider_card = account_content["settings_payload"]["profile_cards"][2] if len(account_content["settings_payload"]["profile_cards"]) > 2 else {"value": "-"}
    return {
        "template": "account_profile.html",
        "account_page": _account_page_meta(
            key="profile",
            kicker="Profile",
            title="Профиль и настройки",
            lead="Способ входа, базовые данные аккаунта и системные карточки кабинета без перемешивания с учебными блоками.",
            metrics=[
                {"label": "Способ входа", "value": provider_card.get("value", "-")},
                {"label": "Курсов", "value": stats["total_courses"]},
                {"label": "Лекций", "value": stats["lecture_records"]},
                {"label": "Преподавателей", "value": stats["teachers_count"]},
            ],
            actions=[
                {"label": "Документы", "href": "/account/documents", "variant": "secondary"},
                {"label": "Выйти", "href": "/logout", "variant": "secondary"},
            ],
        ),
        "account_shell": build_account_shell(active_key="profile", stats=stats),
    }


ACCOUNT_PAGE_BUILDERS = {
    "overview": build_account_overview_page,
    "courses": build_account_courses_page,
    "calendar": build_account_calendar_page,
    "homework": build_account_homework_page,
    "lectures": build_account_lectures_page,
    "teachers": build_account_teachers_page,
    "documents": build_account_documents_page,
    "profile": build_account_profile_page,
}


def build_account_page_payload(
    *,
    page_key: str,
    account_content: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    builder = ACCOUNT_PAGE_BUILDERS.get(page_key) or build_account_overview_page
    return builder(account_content=account_content, agreements=agreements)


def build_account_schedule_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_account_schedule_items(agreements)


def build_account_lectures_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_account_lecture_items(agreements)


def build_account_mock_content(
    agreements: List[Dict[str, Any]],
    *,
    user_display: str,
    payments_enabled: bool,
    user: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return build_account_content(
        agreements,
        user_display=user_display,
        payments_enabled=payments_enabled,
        user=user,
    )
