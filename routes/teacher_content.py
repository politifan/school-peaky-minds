import re
import secrets
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from core import DATA_DIR, load_json, moscow_now, save_json
from routes.account_content import (
    build_homework_admin_items,
    build_teacher_assignment_rows,
    get_teacher_record,
    load_teachers,
)

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


def _format_iso_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return dt.strftime("%d.%m.%Y, %H:%M")


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


def _time_to_minutes(value: str) -> int:
    hour, minute = _time_sort_key(value)
    return hour * 60 + minute


def _minutes_to_time_label(value: int) -> str:
    hours = max(int(value), 0) // 60
    minutes = max(int(value), 0) % 60
    return f"{hours:02d}:{minutes:02d}"


def _days_to_slot_map(days: Any) -> Dict[str, set]:
    normalized_days = _normalize_weekly_days(days)
    slot_map = {key: set() for key in WEEKDAY_KEYS}
    for key in WEEKDAY_KEYS:
        for slot in normalized_days.get(key) or []:
            start_slot = _time_to_minutes(slot["start"]) // 30
            end_slot = _time_to_minutes(slot["end"]) // 30
            if end_slot <= start_slot:
                continue
            slot_map[key].update(range(start_slot, end_slot))
    return slot_map


def _slot_map_to_days(slot_map: Dict[str, set], *, minimum_slots: int = 1) -> Dict[str, List[Dict[str, str]]]:
    days = _empty_weekly_days()
    required_slots = max(int(minimum_slots or 1), 1)
    for key in WEEKDAY_KEYS:
        indexes = sorted(int(value) for value in slot_map.get(key) or [])
        if not indexes:
            continue
        range_start = indexes[0]
        prev = indexes[0]
        for current in indexes[1:]:
            if current == prev + 1:
                prev = current
                continue
            if (prev - range_start + 1) >= required_slots:
                days[key].append(
                    {
                        "start": _minutes_to_time_label(range_start * 30),
                        "end": _minutes_to_time_label((prev + 1) * 30),
                    }
                )
            range_start = current
            prev = current
        if (prev - range_start + 1) >= required_slots:
            days[key].append(
                {
                    "start": _minutes_to_time_label(range_start * 30),
                    "end": _minutes_to_time_label((prev + 1) * 30),
                }
            )
    return days


def _slot_map_summary(slot_map: Dict[str, set], *, minimum_slots: int = 1, limit: int = 4) -> List[str]:
    days = _slot_map_to_days(slot_map, minimum_slots=minimum_slots)
    return build_teacher_availability_summary({"days": days})[:limit]


def _slot_map_has_any(slot_map: Dict[str, set], *, minimum_slots: int = 1) -> bool:
    days = _slot_map_to_days(slot_map, minimum_slots=minimum_slots)
    return any(days.get(key) for key in WEEKDAY_KEYS)


def _intersect_slot_maps(slot_maps: List[Dict[str, set]]) -> Dict[str, set]:
    result = {key: set() for key in WEEKDAY_KEYS}
    if not slot_maps:
        return result
    for key in WEEKDAY_KEYS:
        merged = None
        for slot_map in slot_maps:
            day_slots = set(slot_map.get(key) or set())
            merged = day_slots if merged is None else merged & day_slots
        result[key] = merged or set()
    return result


def _lesson_duration_slot_count(value: Any) -> int:
    try:
        minutes = int(value or 90)
    except Exception:
        minutes = 90
    minutes = min(max(minutes, 30), 240)
    return max(minutes // 30, 1)


def _availability_record_has_slots(record: Optional[Dict[str, Any]], *, minimum_slots: int = 1) -> bool:
    if not isinstance(record, dict):
        return False
    return _slot_map_has_any(_days_to_slot_map(record.get("days")), minimum_slots=minimum_slots)


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


def parse_weekly_availability_form(form: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    days = {}
    for day in WEEKDAY_META:
        slots = []
        for slot_index in range(2):
            start_raw = str(form.get(f"{day['key']}_start_{slot_index}") or "").strip()
            end_raw = str(form.get(f"{day['key']}_end_{slot_index}") or "").strip()
            if not start_raw and not end_raw:
                continue
            if not start_raw or not end_raw:
                return {}, f"{day['label']}: заполните и начало, и конец интервала {slot_index + 1}."
            start = _normalize_time_value(start_raw)
            end = _normalize_time_value(end_raw)
            if not start or not end:
                return {}, f"{day['label']}: используйте время в формате ЧЧ:ММ с шагом 30 минут."
            if _time_sort_key(start) >= _time_sort_key(end):
                return {}, f"{day['label']}: начало интервала {slot_index + 1} должно быть раньше окончания."
            slots.append({"start": start, "end": end})
        ordered = sorted(slots, key=lambda item: (_time_sort_key(item["start"]), _time_sort_key(item["end"])))
        for idx in range(1, len(ordered)):
            if _time_sort_key(ordered[idx]["start"]) < _time_sort_key(ordered[idx - 1]["end"]):
                return {}, f"{day['label']}: интервалы не должны пересекаться."
        days[day["key"]] = ordered
    return days, ""


def _agreement_file_key(agreement: Dict[str, Any]) -> str:
    return str(agreement.get("agreement_file") or agreement.get("_file") or agreement.get("file") or "").strip()


def _agreement_int(agreement: Dict[str, Any], key: str) -> int:
    try:
        return int(agreement.get(key) or 0)
    except Exception:
        return 0


def _agreement_is_group_eligible(agreement: Dict[str, Any]) -> bool:
    status = str(agreement.get("contract_status_key") or agreement.get("contract_status") or "").strip().lower()
    if status == "signed":
        return True
    if str(agreement.get("contract_pdf_url") or "").strip():
        return True
    return _agreement_int(agreement, "paid_lessons") > 0 or _agreement_int(agreement, "attended_lessons") > 0


def _direction_matches_course(direction_key: str, course_name: str) -> bool:
    direction_value = str(direction_key or "").strip().lower()
    course_value = str(course_name or "").strip().lower()
    if not direction_value:
        return True
    if not course_value:
        return False
    if direction_value == course_value:
        return True
    if direction_value in course_value or course_value in direction_value:
        return True
    tokens = [token.strip() for token in re.split(r"[,;/|]+", direction_value) if token.strip()]
    return any(token in course_value or course_value in token for token in tokens)


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
        current_member_ids = list(item.get("member_ids") or [])
        member_ids = [item_id for item_id in current_member_ids if item_id != member_key or item["id"] == key]
        if item["id"] == key:
            if member_key and member_key not in member_ids:
                member_ids.append(member_key)
            updated_group = {
                **item,
                "member_ids": member_ids,
                "updated_at": moscow_now().isoformat(),
            }
            updated_items.append(updated_group)
            continue
        updated_items.append(
            {
                **item,
                "member_ids": member_ids,
                "updated_at": moscow_now().isoformat() if len(member_ids) != len(current_member_ids) else item.get("updated_at"),
            }
        )
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
    indexed_items: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for raw_item in raw_items:
        item = _normalize_student_availability_record(raw_item)
        if not item:
            continue
        dedupe_key = (item["group_id"], item["student_id"])
        indexed_items[dedupe_key] = item
    return sorted(indexed_items.values(), key=lambda item: (item["group_id"], item["student_id"]))


def load_student_availability_map() -> Dict[Tuple[str, str], Dict[str, Any]]:
    return {
        (item["group_id"], item["student_id"]): item
        for item in load_student_availability_items()
    }


def get_student_availability_record(group_id: str, student_id: str) -> Dict[str, Any]:
    group_key = str(group_id or "").strip()
    student_key = str(student_id or "").strip()
    record = load_student_availability_map().get((group_key, student_key))
    if record:
        return record
    return {
        "group_id": group_key,
        "student_id": student_key,
        "days": _empty_weekly_days(),
        "updated_at": "",
    }


def save_student_availability_record(group_id: str, student_id: str, days: Dict[str, List[Dict[str, str]]]) -> Dict[str, Any]:
    group_key = str(group_id or "").strip()
    student_key = str(student_id or "").strip()
    record = {
        "group_id": group_key,
        "student_id": student_key,
        "days": _normalize_weekly_days(days),
        "updated_at": moscow_now().isoformat(),
    }
    records = load_student_availability_map()
    records[(group_key, student_key)] = record
    save_json(
        STUDENT_AVAILABILITY_FILE,
        {
            "items": sorted(
                records.values(),
                key=lambda item: (item["group_id"], item["student_id"]),
            )
        },
    )
    return record


def _agreement_lookup(agreements: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for agreement in agreements:
        agreement_file = _agreement_file_key(agreement)
        if agreement_file:
            lookup[agreement_file] = agreement
    return lookup


def build_candidate_students(
    agreements: List[Dict[str, Any]],
    *,
    direction_key: str = "",
    exclude_member_ids: Optional[set] = None,
) -> List[Dict[str, Any]]:
    items = []
    seen = set()
    excluded_ids = {str(item).strip() for item in (exclude_member_ids or set()) if str(item).strip()}
    for agreement in agreements:
        agreement_file = _agreement_file_key(agreement)
        if not agreement_file or agreement_file in seen or agreement_file in excluded_ids:
            continue
        if not _agreement_is_group_eligible(agreement):
            continue
        course = str(agreement.get("course") or "").strip() or "Курс"
        if not _direction_matches_course(direction_key, course):
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


def _build_group_matching_state(
    group: Dict[str, Any],
    *,
    availability_index: Dict[Tuple[str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    lesson_slots = _lesson_duration_slot_count(group.get("lesson_duration_minutes"))
    teacher_availability = get_teacher_availability_record(group["teacher_id"])
    teacher_slot_map = _days_to_slot_map(teacher_availability.get("days"))
    teacher_has_hours = _slot_map_has_any(teacher_slot_map, minimum_slots=lesson_slots)
    answered_records = [
        availability_index[(group["id"], member_id)]
        for member_id in group["member_ids"]
        if (group["id"], member_id) in availability_index
        and _availability_record_has_slots(availability_index[(group["id"], member_id)])
    ]
    answered_slot_maps = [_days_to_slot_map(item.get("days")) for item in answered_records]
    common_slot_map = (
        _intersect_slot_maps([teacher_slot_map, *answered_slot_maps])
        if teacher_has_hours and answered_slot_maps
        else {key: set() for key in WEEKDAY_KEYS}
    )
    common_windows_summary = _slot_map_summary(common_slot_map, minimum_slots=lesson_slots, limit=4)
    answered_count = len(answered_records)
    members_count = len(group["member_ids"])
    all_answered = bool(members_count) and answered_count == members_count
    if not members_count:
        match_status = "Нет участников"
        scope_label = ""
    elif not teacher_has_hours:
        match_status = "Сначала задайте рабочие часы"
        scope_label = "Рабочие часы преподавателя пока не заполнены."
    elif answered_count == 0:
        match_status = "Ждём ответы по времени"
        scope_label = "Сначала хотя бы один ученик должен указать доступность."
    elif all_answered and common_windows_summary:
        match_status = f"Найдено {len(common_windows_summary)} общих окна"
        scope_label = "Подходит всей группе и попадает в рабочие часы преподавателя."
    elif all_answered:
        match_status = "Общих окон пока нет"
        scope_label = "Все участники ответили, но пересечения по времени пока нет."
    elif common_windows_summary:
        match_status = f"Есть окна для {answered_count} из {members_count}"
        scope_label = f"Подходит всем, кто уже отметил время: {answered_count} из {members_count}."
    else:
        match_status = f"Ответили {answered_count} из {members_count}"
        scope_label = "Общее окно появится после новых ответов или корректировки часов."
    return {
        "teacher_availability": teacher_availability,
        "teacher_availability_summary": build_teacher_availability_summary(teacher_availability),
        "teacher_has_hours": teacher_has_hours,
        "answered_count": answered_count,
        "members_count": members_count,
        "all_answered": all_answered,
        "common_windows_summary": common_windows_summary,
        "common_windows_count": len(common_windows_summary),
        "match_status": match_status,
        "match_scope_label": scope_label,
    }


def build_student_group_items(current_agreements: List[Dict[str, Any]], agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    current_agreement_map = _agreement_lookup(current_agreements)
    current_member_ids = set(current_agreement_map.keys())
    if not current_member_ids:
        return []
    agreement_map = _agreement_lookup(agreements)
    teacher_map = {item["id"]: item for item in load_teachers()}
    availability_index = load_student_availability_map()
    items = []
    for group in load_study_groups():
        own_member_ids = [member_id for member_id in group["member_ids"] if member_id in current_member_ids]
        if not own_member_ids:
            continue
        matching_state = _build_group_matching_state(group, availability_index=availability_index)
        teacher_record = teacher_map.get(group["teacher_id"], {})
        teacher_slot_map = _days_to_slot_map(matching_state["teacher_availability"].get("days"))
        lesson_slots = _lesson_duration_slot_count(group.get("lesson_duration_minutes"))
        members = []
        for member_id in group["member_ids"]:
            agreement = agreement_map.get(member_id, {})
            availability = availability_index.get((group["id"], member_id))
            members.append(
                {
                    "agreement_file": member_id,
                    "student_name": str(agreement.get("full_name") or agreement.get("name") or "").strip() or "Студент",
                    "course": str(agreement.get("course") or "").strip() or group["direction_key"] or "Курс",
                    "availability_ready": _availability_record_has_slots(availability),
                    "availability_updated_label": _format_iso_label(availability.get("updated_at")) if availability else "",
                    "is_current_user": member_id in current_member_ids,
                }
            )
        members.sort(key=lambda item: (not item["is_current_user"], item["student_name"].lower()))
        for student_id in own_member_ids:
            agreement = current_agreement_map.get(student_id, {})
            student_availability = get_student_availability_record(group["id"], student_id)
            student_slot_map = _days_to_slot_map(student_availability.get("days"))
            student_teacher_overlap = (
                _intersect_slot_maps([teacher_slot_map, student_slot_map])
                if matching_state["teacher_has_hours"] and _slot_map_has_any(student_slot_map)
                else {key: set() for key in WEEKDAY_KEYS}
            )
            teacher_student_overlap_summary = _slot_map_summary(student_teacher_overlap, minimum_slots=lesson_slots, limit=4)
            items.append(
                {
                    **group,
                    "teacher": teacher_record,
                    "teacher_name": teacher_record.get("name") or "Преподаватель",
                    "teacher_role": teacher_record.get("role") or "Преподаватель",
                    "teacher_contact_url": _teacher_contact_url(teacher_record),
                    "teacher_initials": teacher_record.get("initials") or "PM",
                    "teacher_accent": teacher_record.get("accent") or "violet",
                    "student_id": student_id,
                    "course": str(agreement.get("course") or "").strip() or group["direction_key"] or "Курс",
                    "student_availability_rows": build_teacher_availability_rows(student_availability),
                    "student_availability_summary": build_teacher_availability_summary(student_availability),
                    "student_availability_ready": _slot_map_has_any(student_slot_map),
                    "teacher_availability_summary": matching_state["teacher_availability_summary"],
                    "teacher_hours_ready": matching_state["teacher_has_hours"],
                    "teacher_student_overlap_summary": teacher_student_overlap_summary,
                    "teacher_student_overlap_count": len(teacher_student_overlap_summary),
                    "common_windows_summary": matching_state["common_windows_summary"],
                    "common_windows_count": matching_state["common_windows_count"],
                    "common_windows_scope_label": matching_state["match_scope_label"],
                    "all_members_ready": matching_state["all_answered"],
                    "availability_count": matching_state["answered_count"],
                    "members_count": matching_state["members_count"],
                    "members_waiting_count": max(matching_state["members_count"] - matching_state["answered_count"], 0),
                    "match_status": matching_state["match_status"],
                    "members": members,
                    "updated_at_label": _format_iso_label(student_availability.get("updated_at")),
                }
            )
    items.sort(key=lambda item: (item["status"] == "archived", item["title"].lower(), item["course"].lower()))
    return items


def _build_teacher_group_items_v2(teacher_id: str, agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    availability_index = load_student_availability_map()
    agreement_map = _agreement_lookup(agreements)
    all_groups = load_study_groups()
    items = []
    for group in all_groups:
        if group["teacher_id"] != teacher_id:
            continue
        matching_state = _build_group_matching_state(group, availability_index=availability_index)
        members = []
        member_courses = set()
        filled_count = 0
        last_updated = ""
        for member_id in group["member_ids"]:
            agreement = agreement_map.get(member_id, {})
            availability = availability_index.get((group["id"], member_id))
            availability_ready = _availability_record_has_slots(availability)
            if availability_ready:
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
                    "availability_ready": availability_ready,
                    "availability_updated_at": str(availability.get("updated_at") or "") if availability else "",
                    "availability_updated_label": _format_iso_label(availability.get("updated_at")) if availability else "",
                }
            )
        members.sort(key=lambda item: (item["course"].lower(), item["student_name"].lower()))
        members_count = len(members)
        progress_percent = int(round((filled_count / members_count) * 100)) if members_count else 0
        occupied_member_ids = {
            member_id
            for other_group in all_groups
            if other_group["id"] != group["id"]
            for member_id in other_group.get("member_ids") or []
        }
        items.append(
            {
                **group,
                "members": members,
                "members_count": members_count,
                "availability_count": filled_count,
                "match_status": matching_state["match_status"],
                "match_scope_label": matching_state["match_scope_label"],
                "progress_percent": progress_percent,
                "progress_label": f"{progress_percent}%" if members_count else "0%",
                "courses": sorted(member_courses) if member_courses else ([group["direction_key"]] if group["direction_key"] else []),
                "last_availability_update": last_updated,
                "last_availability_update_label": _format_iso_label(last_updated),
                "group_edit_href": f"/teacher?view=groups&teacher_id={group['teacher_id']}&group_id={group['id']}",
                "teacher_availability_summary": matching_state["teacher_availability_summary"],
                "teacher_has_hours": matching_state["teacher_has_hours"],
                "common_windows_summary": matching_state["common_windows_summary"],
                "common_windows_count": matching_state["common_windows_count"],
                "all_members_ready": matching_state["all_answered"],
                "candidate_students": [
                    item
                    for item in build_candidate_students(
                        agreements,
                        direction_key=group["direction_key"],
                        exclude_member_ids=occupied_member_ids,
                    )
                    if item["agreement_file"] not in group["member_ids"]
                ],
            }
        )
    items.sort(key=lambda item: (item["status"] == "archived", item["title"].lower()))
    return items


def build_teacher_group_items(teacher_id: str, agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _build_teacher_group_items_v2(teacher_id, agreements)

    availability_index = load_student_availability_map()
    agreement_map = _agreement_lookup(agreements)
    all_groups = load_study_groups()
    items = []
    for group in all_groups:
        if group["teacher_id"] != teacher_id:
            continue
        matching_state = _build_group_matching_state(group, availability_index=availability_index)
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
                    "availability_updated_label": _format_iso_label(availability.get("updated_at")) if availability else "",
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
        progress_percent = int(round((filled_count / members_count) * 100)) if members_count else 0
        items.append(
            {
                **group,
                "members": members,
                "members_count": members_count,
                "availability_count": filled_count,
                "match_status": match_status,
                "progress_percent": progress_percent,
                "progress_label": f"{progress_percent}%" if members_count else "0%",
                "courses": sorted(member_courses) if member_courses else ([group["direction_key"]] if group["direction_key"] else []),
                "last_availability_update": last_updated,
                "last_availability_update_label": _format_iso_label(last_updated),
                "group_edit_href": f"/teacher?view=groups&teacher_id={group['teacher_id']}&group_id={group['id']}",
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
                    "availability_updated_label": member.get("availability_updated_label", ""),
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
            "today_lessons": [],
            "next_lessons": [],
            "attention_items": [],
            "ready_to_schedule_groups": [],
            "availability_record": {"days": _empty_weekly_days(), "updated_at": ""},
            "availability_rows": build_teacher_availability_rows({"days": _empty_weekly_days()}),
            "availability_summary": [],
            "courses": [],
            "summary": {
                "hours_days_count": 0,
                "hours_slots_count": 0,
                "students_ready_count": 0,
                "students_waiting_count": 0,
                "groups_ready_count": 0,
                "groups_waiting_count": 0,
                "availability_updated_label": "",
                "latest_student_response_label": "",
            },
        }
    teacher_id = teacher_record["id"]
    assignment_rows = [item for item in build_teacher_assignment_rows(agreements) if item["teacher_id"] == teacher_id]
    homework_items = [item for item in build_homework_admin_items(agreements) if item.get("teacher_id") == teacher_id]
    group_items = build_teacher_group_items(teacher_id, agreements)
    student_rows = build_teacher_student_rows(group_items)
    availability_record = get_teacher_availability_record(teacher_id)
    availability_rows = build_teacher_availability_rows(availability_record)
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
        | {
            str(course or "").strip()
            for group in group_items
            for course in (group.get("courses") or [group.get("direction_key")])
            if str(course or "").strip()
        }
        | {
            str(course or "").strip()
            for course in (teacher_record.get("disciplines") or [])
            if str(course or "").strip()
        }
    )
    students_ready_count = sum(1 for item in student_rows if item.get("availability_ready"))
    hours_days_count = sum(1 for row in availability_rows if not row["is_empty"])
    hours_slots_count = sum(len(row["slots"]) for row in availability_rows)
    groups_ready_count = sum(
        1
        for item in group_items
        if item["members_count"] and item["availability_count"] == item["members_count"]
    )
    today_key = moscow_now().date().isoformat()
    today_lessons = [item for item in assignment_rows if item.get("date") == today_key]
    next_lessons = [item for item in assignment_rows if item.get("date", "") >= today_key]
    ready_to_schedule_groups = [
        item
        for item in group_items
        if item.get("common_windows_count") and not str(item.get("final_slot") or "").strip()
    ]
    groups_without_windows = [
        item
        for item in group_items
        if item.get("members_count") and item.get("all_members_ready") and not item.get("common_windows_count")
    ]
    profile_missing = []
    if not str(teacher_record.get("role") or "").strip():
        profile_missing.append("роль")
    if not (teacher_record.get("disciplines") or []) and not str(teacher_record.get("speciality") or "").strip():
        profile_missing.append("дисциплины")
    if not str(teacher_record.get("bio") or "").strip():
        profile_missing.append("описание")
    if not str(teacher_record.get("email") or "").strip() and not str(teacher_record.get("telegram") or "").strip():
        profile_missing.append("контакт")
    latest_student_response = max(
        (item["last_availability_update"] for item in group_items if item.get("last_availability_update")),
        default="",
    )
    attention_items = []
    if profile_missing:
        attention_items.append(
            {
                "title": "Заполнить профиль",
                "text": "Не хватает: " + ", ".join(profile_missing) + ".",
                "href": "/teacher?view=profile",
                "view": "profile",
                "tone": "warm",
                "metric": len(profile_missing),
            }
        )
    if not availability_summary:
        attention_items.append(
            {
                "title": "Настроить рабочие часы",
                "text": "Без недельного шаблона ученики не увидят корректные окна.",
                "href": "/teacher?view=hours",
                "view": "hours",
                "tone": "danger",
                "metric": 0,
            }
        )
    waiting_students_count = max(len(student_rows) - students_ready_count, 0)
    if waiting_students_count:
        attention_items.append(
            {
                "title": "Дождаться доступности учеников",
                "text": f"Ещё {waiting_students_count} учеников не указали удобное время.",
                "href": "/teacher?view=students",
                "view": "students",
                "tone": "warm",
                "metric": waiting_students_count,
            }
        )
    if ready_to_schedule_groups:
        attention_items.append(
            {
                "title": "Выбрать финальный слот",
                "text": f"В {len(ready_to_schedule_groups)} группах уже есть общие окна.",
                "href": "/teacher?view=groups",
                "view": "groups",
                "tone": "good",
                "metric": len(ready_to_schedule_groups),
            }
        )
    if groups_without_windows:
        attention_items.append(
            {
                "title": "Разобрать конфликт времени",
                "text": f"В {len(groups_without_windows)} группах все ответили, но общего окна нет.",
                "href": "/teacher?view=groups",
                "view": "groups",
                "tone": "danger",
                "metric": len(groups_without_windows),
            }
        )
    if not group_items:
        attention_items.append(
            {
                "title": "Создать первую группу",
                "text": "После создания группы можно добавить учеников и собрать доступность.",
                "href": "/teacher?view=groups",
                "view": "groups",
                "tone": "neutral",
                "metric": 0,
            }
        )
    return {
        "kpis": [
            {"label": "Групп", "value": len(group_items), "note": "Активные и формирующиеся составы"},
            {"label": "Учеников", "value": len(student_rows), "note": "Во всех группах преподавателя"},
            {"label": "Занятий в месяце", "value": len(assignment_rows), "note": "Назначения из текущего расписания"},
            {"label": "Домашек", "value": len(homework_items), "note": "Задания, закреплённые за преподавателем"},
        ],
        "upcoming_lessons": (next_lessons or assignment_rows)[:8],
        "today_lessons": today_lessons,
        "next_lessons": next_lessons[:8],
        "homework_items": homework_items[:8],
        "group_items": group_items,
        "student_rows": student_rows,
        "attention_items": attention_items[:6],
        "ready_to_schedule_groups": ready_to_schedule_groups[:6],
        "availability_record": availability_record,
        "availability_rows": availability_rows,
        "availability_summary": availability_summary,
        "courses": courses,
        "summary": {
            "hours_days_count": hours_days_count,
            "hours_slots_count": hours_slots_count,
            "students_ready_count": students_ready_count,
            "students_waiting_count": waiting_students_count,
            "groups_ready_count": groups_ready_count,
            "groups_waiting_count": max(len(group_items) - groups_ready_count, 0),
            "availability_updated_label": _format_iso_label(availability_record.get("updated_at")),
            "latest_student_response_label": _format_iso_label(latest_student_response),
        },
    }


def build_teacher_public_profile(
    teacher_record: Dict[str, Any],
    agreements: List[Dict[str, Any]],
) -> Dict[str, Any]:
    dashboard = build_teacher_dashboard(teacher_record, agreements)
    group_items = dashboard["group_items"]
    students_count = len({item["agreement_file"] for item in dashboard["student_rows"]})
    expertise = teacher_record.get("expertise") if isinstance(teacher_record.get("expertise"), list) else []
    courses = []
    for course in [
        *(teacher_record.get("disciplines") or []),
        *dashboard["courses"],
        str(teacher_record.get("speciality") or "").strip(),
    ]:
        course_name = str(course or "").strip()
        if course_name and course_name not in courses:
            courses.append(course_name)
    primary_course = courses[0] if courses else str(teacher_record.get("speciality") or "").strip()
    next_lesson = dashboard["upcoming_lessons"][0] if dashboard["upcoming_lessons"] else {}
    groups_with_windows = sum(1 for item in group_items if item.get("common_windows_count"))
    active_groups = [item for item in group_items if item.get("status") != "archived"]
    return {
        **teacher_record,
        "contact_url": _teacher_contact_url(teacher_record),
        "students_count": students_count,
        "groups_count": len(group_items),
        "active_groups_count": len(active_groups),
        "groups_with_windows_count": groups_with_windows,
        "upcoming_count": len(dashboard["upcoming_lessons"]),
        "courses": courses,
        "primary_course": primary_course,
        "expertise": expertise,
        "availability_summary": dashboard["availability_summary"][:4],
        "availability_updated_label": dashboard["summary"]["availability_updated_label"],
        "next_lesson_label": (
            f"{next_lesson.get('date_label')} · {next_lesson.get('time')}"
            if next_lesson.get("date_label") and next_lesson.get("time")
            else ""
        ),
        "public_facts": [
            {
                "label": "Фокус",
                "value": primary_course or "Индивидуальный трек",
                "text": "Направление, по которому преподаватель сейчас ведёт группы и занятия.",
            },
            {
                "label": "Состав",
                "value": f"{len(active_groups)} активных групп",
                "text": "Группы собираются вокруг расписания преподавателя и доступности учеников.",
            },
            {
                "label": "Расписание",
                "value": f"{groups_with_windows} групп с окнами",
                "text": "Общие слоты рассчитываются по рабочим часам и ответам учеников.",
            },
        ],
        "match_note": "Рабочие часы и группы синхронизируются внутри кабинета преподавателя.",
    }
