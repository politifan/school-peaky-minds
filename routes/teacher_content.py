import re
import secrets
from typing import Any, Dict, List, Optional, Tuple

from core import DATA_DIR, load_json, moscow_now, save_json
from routes.account_content import build_homework_admin_items, build_teacher_assignment_rows, get_teacher_record, load_teachers

TEACHER_AVAILABILITY_FILE = DATA_DIR / "teacher_availability.json"
STUDY_GROUPS_FILE = DATA_DIR / "study_groups.json"
STUDENT_AVAILABILITY_FILE = DATA_DIR / "student_availability.json"

WEEKDAY_META = [
    {"key": "mon", "short": "Пн", "label": "Понедельник"},
    {"key": "tue", "short": "Вт", "label": "Вторник"},
    {"key": "wed", "short": "Ср", "label": "Среда"},
    {"key": "thu", "short": "Чт", "label": "Четверг"},
    {"key": "fri", "short": "Пт", "label": "Пятница"},
    {"key": "sat", "short": "Сб", "label": "Суббота"},
    {"key": "sun", "short": "Вс", "label": "Воскресенье"},
]
WEEKDAY_KEYS = tuple(item["key"] for item in WEEKDAY_META)
GROUP_STATUS_META = {
    "forming": "Формируется",
    "matching": "Собираем время",
    "scheduled": "Слот выбран",
    "active": "Активна",
    "archived": "Архив",
}


def _teacher_contact_url(item: Dict[str, Any]) -> str:
    telegram = str(item.get("telegram") or "").strip()
    if telegram:
        return f"https://t.me/{telegram.lstrip('@')}"
    email = str(item.get("email") or "").strip()
    if email:
        return f"mailto:{email}"
    return ""


def _empty_weekly_days() -> Dict[str, List[Dict[str, str]]]:
    return {key: [] for key in WEEKDAY_KEYS}


def _normalize_time_value(value: Any) -> str:
    raw = str(value or "").strip()
    if re.match(r"^\d{2}:\d{2}$", raw):
        hour = int(raw[:2])
        minute = int(raw[3:5])
        if 0 <= hour <= 23 and minute in {0, 30}:
            return raw
    return ""


def _time_sort_key(value: str) -> Tuple[int, int]:
    return int(value[:2]), int(value[3:5])


def _normalize_interval(item: Any) -> Optional[Dict[str, str]]:
    if not isinstance(item, dict):
        return None
    start = _normalize_time_value(item.get("start"))
    end = _normalize_time_value(item.get("end"))
    if not start or not end or _time_sort_key(start) >= _time_sort_key(end):
        return None
    return {"start": start, "end": end}


def _normalize_weekly_days(value: Any) -> Dict[str, List[Dict[str, str]]]:
    days = _empty_weekly_days()
    if not isinstance(value, dict):
        return days
    for key in WEEKDAY_KEYS:
        raw_slots = value.get(key)
        if not isinstance(raw_slots, list):
            continue
        slots = []
        seen = set()
        for raw_slot in raw_slots:
            slot = _normalize_interval(raw_slot)
            if not slot:
                continue
            dedupe_key = (slot["start"], slot["end"])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            slots.append(slot)
        slots.sort(key=lambda item: (_time_sort_key(item["start"]), _time_sort_key(item["end"])))
        days[key] = slots
    return days


def _normalize_teacher_availability_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    teacher_id = str(item.get("teacher_id") or "").strip()
    if not teacher_id:
        return None
    updated_at = str(item.get("updated_at") or "").strip() or moscow_now().isoformat()
    return {
        "teacher_id": teacher_id,
        "days": _normalize_weekly_days(item.get("days")),
        "updated_at": updated_at,
    }


def load_teacher_availability_map() -> Dict[str, Dict[str, Any]]:
    data = load_json(TEACHER_AVAILABILITY_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        return {}
    records: Dict[str, Dict[str, Any]] = {}
    for raw_item in raw_items:
        record = _normalize_teacher_availability_record(raw_item)
        if not record:
            continue
        records[record["teacher_id"]] = record
    return records


def get_teacher_availability_record(teacher_id: str) -> Dict[str, Any]:
    key = str(teacher_id or "").strip()
    record = load_teacher_availability_map().get(key)
    if record:
        return record
    return {
        "teacher_id": key,
        "days": _empty_weekly_days(),
        "updated_at": "",
    }


def save_teacher_availability_record(teacher_id: str, days: Dict[str, List[Dict[str, str]]]) -> Dict[str, Any]:
    key = str(teacher_id or "").strip()
    records = load_teacher_availability_map()
    record = {
        "teacher_id": key,
        "days": _normalize_weekly_days(days),
        "updated_at": moscow_now().isoformat(),
    }
    records[key] = record
    payload = {"items": sorted(records.values(), key=lambda item: item["teacher_id"])}
    save_json(TEACHER_AVAILABILITY_FILE, payload)
    return record


def build_teacher_availability_rows(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    days = record.get("days") if isinstance(record, dict) else {}
    if not isinstance(days, dict):
        days = {}
    for meta in WEEKDAY_META:
        slots = days.get(meta["key"]) if isinstance(days.get(meta["key"]), list) else []
        editor_slots = slots[:2] + [{"start": "", "end": ""}] * max(0, 2 - len(slots[:2]))
        rows.append(
            {
                **meta,
                "slots": slots,
                "slot_labels": [f'{slot["start"]} - {slot["end"]}' for slot in slots],
                "editor_slots": editor_slots[:2],
                "is_empty": not slots,
            }
        )
    return rows


def build_teacher_availability_summary(record: Dict[str, Any]) -> List[str]:
    rows = build_teacher_availability_rows(record)
    summary = []
    for row in rows:
        if row["slot_labels"]:
            summary.append(f'{row["short"]}: {", ".join(row["slot_labels"])}')
    return summary


def _normalize_group_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    group_id = str(item.get("id") or "").strip() or f"group-{secrets.token_hex(6)}"
    teacher_id = str(item.get("teacher_id") or "").strip()
    title = str(item.get("title") or "").strip()
    if not teacher_id or not title:
        return None
    status = str(item.get("status") or "forming").strip()
    if status not in GROUP_STATUS_META:
        status = "forming"
    member_ids = item.get("member_ids")
    if not isinstance(member_ids, list):
        member_ids = item.get("members")
    normalized_member_ids = []
    seen_members = set()
    for raw_member_id in member_ids or []:
        member_id = str(raw_member_id or "").strip()
        if not member_id or member_id in seen_members:
            continue
        seen_members.add(member_id)
        normalized_member_ids.append(member_id)
    lesson_duration_minutes = 90
    try:
        lesson_duration_minutes = int(item.get("lesson_duration_minutes") or 90)
    except Exception:
        lesson_duration_minutes = 90
    lesson_duration_minutes = min(max(lesson_duration_minutes, 30), 240)
    created_at = str(item.get("created_at") or "").strip() or moscow_now().isoformat()
    updated_at = str(item.get("updated_at") or "").strip() or created_at
    return {
        "id": group_id,
        "teacher_id": teacher_id,
        "direction_key": str(item.get("direction_key") or "").strip(),
        "title": title,
        "lesson_duration_minutes": lesson_duration_minutes,
        "status": status,
        "status_label": GROUP_STATUS_META[status],
        "final_slot": str(item.get("final_slot") or "").strip(),
        "member_ids": normalized_member_ids,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def load_study_groups() -> List[Dict[str, Any]]:
    data = load_json(STUDY_GROUPS_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        return []
    items = []
    seen = set()
    for raw_item in raw_items:
        group = _normalize_group_record(raw_item)
        if not group or group["id"] in seen:
            continue
        seen.add(group["id"])
        items.append(group)
    items.sort(key=lambda item: (item["status"] == "archived", item["title"].lower()))
    return items


def save_study_groups(items: List[Dict[str, Any]]) -> None:
    save_json(STUDY_GROUPS_FILE, {"items": items})


def get_study_group(group_id: str) -> Optional[Dict[str, Any]]:
    key = str(group_id or "").strip()
    if not key:
        return None
    for item in load_study_groups():
        if item["id"] == key:
            return item
    return None


def upsert_study_group(
    *,
    teacher_id: str,
    title: str,
    direction_key: str = "",
    lesson_duration_minutes: int = 90,
    status: str = "forming",
    final_slot: str = "",
    group_id: str = "",
) -> Dict[str, Any]:
    items = load_study_groups()
    existing = None
    record_id = str(group_id or "").strip()
    if record_id:
        for item in items:
            if item["id"] == record_id:
                existing = item
                break
    now_iso = moscow_now().isoformat()
    payload = _normalize_group_record(
        {
            "id": record_id or f"group-{secrets.token_hex(6)}",
            "teacher_id": teacher_id or (existing.get("teacher_id") if existing else ""),
            "title": title,
            "direction_key": direction_key or (existing.get("direction_key") if existing else ""),
            "lesson_duration_minutes": lesson_duration_minutes or (existing.get("lesson_duration_minutes") if existing else 90),
            "status": status or (existing.get("status") if existing else "forming"),
            "final_slot": final_slot,
            "member_ids": existing.get("member_ids") if existing else [],
            "created_at": existing.get("created_at") if existing else now_iso,
            "updated_at": now_iso,
        }
    )
    if not payload:
        raise ValueError("Укажите название группы")
    updated = [item for item in items if item["id"] != payload["id"]]
    updated.append(payload)
    save_study_groups(updated)
    return payload


def add_group_member(group_id: str, agreement_file: str) -> Optional[Dict[str, Any]]:
    items = load_study_groups()
    updated_items = []
    updated_group = None
    key = str(group_id or "").strip()
    member_key = str(agreement_file or "").strip()
    for item in items:
        if item["id"] != key:
            updated_items.append(item)
            continue
        member_ids = list(item.get("member_ids") or [])
        if member_key and member_key not in member_ids:
            member_ids.append(member_key)
        updated_group = {
            **item,
            "member_ids": member_ids,
            "updated_at": moscow_now().isoformat(),
        }
        updated_items.append(updated_group)
    if updated_group:
        save_study_groups(updated_items)
    return updated_group


def remove_group_member(group_id: str, agreement_file: str) -> Optional[Dict[str, Any]]:
    items = load_study_groups()
    updated_items = []
    updated_group = None
    key = str(group_id or "").strip()
    member_key = str(agreement_file or "").strip()
    for item in items:
        if item["id"] != key:
            updated_items.append(item)
            continue
        member_ids = [item_id for item_id in item.get("member_ids") or [] if item_id != member_key]
        updated_group = {
            **item,
            "member_ids": member_ids,
            "updated_at": moscow_now().isoformat(),
        }
        updated_items.append(updated_group)
    if updated_group:
        save_study_groups(updated_items)
    return updated_group


def _normalize_student_availability_record(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    group_id = str(item.get("group_id") or "").strip()
    student_id = str(item.get("student_id") or "").strip()
    if not group_id or not student_id:
        return None
    return {
        "group_id": group_id,
        "student_id": student_id,
        "days": _normalize_weekly_days(item.get("days")),
        "updated_at": str(item.get("updated_at") or "").strip() or moscow_now().isoformat(),
    }


def load_student_availability_items() -> List[Dict[str, Any]]:
    data = load_json(STUDENT_AVAILABILITY_FILE, {"items": []})
    raw_items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(raw_items, list):
        return []
    items = []
    seen = set()
    for raw_item in raw_items:
        item = _normalize_student_availability_record(raw_item)
        if not item:
            continue
        dedupe_key = (item["group_id"], item["student_id"])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        items.append(item)
    return items


def _agreement_lookup(agreements: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for agreement in agreements:
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or agreement.get("file") or "").strip()
        if agreement_file:
            lookup[agreement_file] = agreement
    return lookup


def build_candidate_students(agreements: List[Dict[str, Any]], *, direction_key: str = "") -> List[Dict[str, Any]]:
    direction_value = str(direction_key or "").strip().lower()
    items = []
    seen = set()
    for agreement in agreements:
        agreement_file = str(agreement.get("agreement_file") or agreement.get("_file") or agreement.get("file") or "").strip()
        if not agreement_file or agreement_file in seen:
            continue
        course = str(agreement.get("course") or "").strip() or "Курс"
        if direction_value and course.lower() != direction_value:
            continue
        seen.add(agreement_file)
        items.append(
            {
                "agreement_file": agreement_file,
                "student_name": str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Без имени",
                "course": course,
                "email": str(agreement.get("email") or "").strip(),
                "phone": str(agreement.get("phone") or "").strip(),
                "current_module": str(agreement.get("current_module") or "").strip(),
            }
        )
    items.sort(key=lambda item: (item["course"].lower(), item["student_name"].lower()))
    return items


def build_teacher_group_items(teacher_id: str, agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    availability_index = {
        (item["group_id"], item["student_id"]): item
        for item in load_student_availability_items()
    }
    agreement_map = _agreement_lookup(agreements)
    items = []
    for group in load_study_groups():
        if group["teacher_id"] != teacher_id:
            continue
        members = []
        member_courses = set()
        filled_count = 0
        last_updated = ""
        for member_id in group["member_ids"]:
            agreement = agreement_map.get(member_id, {})
            availability = availability_index.get((group["id"], member_id))
            if availability:
                filled_count += 1
                updated_at = str(availability.get("updated_at") or "")
                if updated_at and (not last_updated or updated_at > last_updated):
                    last_updated = updated_at
            course = str(agreement.get("course") or "").strip() or group["direction_key"] or "Курс"
            member_courses.add(course)
            members.append(
                {
                    "agreement_file": member_id,
                    "student_name": str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Студент без имени",
                    "course": course,
                    "email": str(agreement.get("email") or "").strip(),
                    "phone": str(agreement.get("phone") or "").strip(),
                    "availability_ready": bool(availability),
                    "availability_updated_at": str(availability.get("updated_at") or "") if availability else "",
                }
            )
        members.sort(key=lambda item: (item["course"].lower(), item["student_name"].lower()))
        members_count = len(members)
        if not members_count:
            match_status = "Нет участников"
        elif filled_count == 0:
            match_status = "Ждём ответы по времени"
        elif filled_count < members_count:
            match_status = f"Ответили {filled_count} из {members_count}"
        else:
            match_status = "Можно считать общие окна"
        items.append(
            {
                **group,
                "members": members,
                "members_count": members_count,
                "availability_count": filled_count,
                "match_status": match_status,
                "courses": sorted(member_courses) if member_courses else ([group["direction_key"]] if group["direction_key"] else []),
                "last_availability_update": last_updated,
                "candidate_students": [
                    item
                    for item in build_candidate_students(agreements, direction_key=group["direction_key"])
                    if item["agreement_file"] not in group["member_ids"]
                ],
            }
        )
    items.sort(key=lambda item: (item["status"] == "archived", item["title"].lower()))
    return items


def build_teacher_student_rows(group_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for group in group_items:
        for member in group["members"]:
            rows.append(
                {
                    **member,
                    "group_id": group["id"],
                    "group_title": group["title"],
                    "group_status": group["status_label"],
                    "final_slot": group["final_slot"] or "Пока не зафиксирован",
                }
            )
    rows.sort(key=lambda item: (item["student_name"].lower(), item["group_title"].lower()))
    return rows


def resolve_teacher_record(
    *,
    teacher_id: str = "",
    access_entry: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    direct_teacher_id = str(teacher_id or "").strip()
    if direct_teacher_id:
        return get_teacher_record(direct_teacher_id)
    mapped_teacher_id = str((access_entry or {}).get("teacher_id") or "").strip()
    if mapped_teacher_id:
        return get_teacher_record(mapped_teacher_id)
    access_email = str((access_entry or {}).get("email") or "").strip().lower()
    if not access_email:
        user_id = str((access_entry or {}).get("user_id") or "").strip()
        if user_id.startswith("email:"):
            access_email = user_id.split(":", 1)[1].strip().lower()
    if access_email:
        for item in load_teachers():
            if str(item.get("email") or "").strip().lower() == access_email:
                return item
    return None


def build_teacher_dashboard(
    teacher_record: Optional[Dict[str, Any]],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not teacher_record:
        return {
            "kpis": [],
            "upcoming_lessons": [],
            "homework_items": [],
            "group_items": [],
            "student_rows": [],
            "availability_record": {"days": _empty_weekly_days(), "updated_at": ""},
            "availability_rows": build_teacher_availability_rows({"days": _empty_weekly_days()}),
            "availability_summary": [],
            "courses": [],
        }
    teacher_id = teacher_record["id"]
    assignment_rows = [item for item in build_teacher_assignment_rows(agreements) if item["teacher_id"] == teacher_id]
    homework_items = [item for item in build_homework_admin_items(agreements) if item.get("teacher_id") == teacher_id]
    group_items = build_teacher_group_items(teacher_id, agreements)
    student_rows = build_teacher_student_rows(group_items)
    availability_record = get_teacher_availability_record(teacher_id)
    availability_summary = build_teacher_availability_summary(availability_record)
    courses = sorted(
        {
            str(item.get("course") or "").strip()
            for item in assignment_rows
            if str(item.get("course") or "").strip()
        }
        | {
            str(item.get("course") or "").strip()
            for item in student_rows
            if str(item.get("course") or "").strip()
        }
    )
    return {
        "kpis": [
            {"label": "Групп", "value": len(group_items), "note": "Активные и формирующиеся составы"},
            {"label": "Учеников", "value": len(student_rows), "note": "Во всех группах преподавателя"},
            {"label": "Занятий в месяце", "value": len(assignment_rows), "note": "Назначения из текущего расписания"},
            {"label": "Домашек", "value": len(homework_items), "note": "Задания, закреплённые за преподавателем"},
        ],
        "upcoming_lessons": assignment_rows[:8],
        "homework_items": homework_items[:8],
        "group_items": group_items,
        "student_rows": student_rows,
        "availability_record": availability_record,
        "availability_rows": build_teacher_availability_rows(availability_record),
        "availability_summary": availability_summary,
        "courses": courses,
    }


def build_teacher_public_profile(
    teacher_record: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    dashboard = build_teacher_dashboard(teacher_record, agreements)
    group_items = dashboard["group_items"]
    students_count = len({item["agreement_file"] for item in dashboard["student_rows"]})
    expertise = teacher_record.get("expertise") if isinstance(teacher_record.get("expertise"), list) else []
    courses = dashboard["courses"] or ([teacher_record["speciality"]] if teacher_record.get("speciality") else [])
    return {
        **teacher_record,
        "contact_url": _teacher_contact_url(teacher_record),
        "students_count": students_count,
        "groups_count": len(group_items),
        "upcoming_count": len(dashboard["upcoming_lessons"]),
        "courses": courses,
        "expertise": expertise,
        "availability_summary": dashboard["availability_summary"][:4],
        "match_note": "Рабочие часы и группы синхронизируются внутри кабинета преподавателя.",
    }
