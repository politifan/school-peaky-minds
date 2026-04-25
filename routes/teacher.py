import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND

from core import (
    get_current_user,
    get_teacher_access_entry,
    is_admin_user,
    load_agreements,
    render,
    teacher_required,
)
from routes.account_content import get_teacher_record, load_teachers, upsert_teacher_record
from routes.teacher_content import (
    GROUP_STATUS_META,
    WEEKDAY_META,
    add_group_member,
    build_candidate_students,
    build_teacher_dashboard,
    build_teacher_public_profile,
    get_study_group,
    load_study_groups,
    remove_group_member,
    resolve_teacher_record,
    save_teacher_availability_record,
    upsert_study_group,
)

router = APIRouter()

TEACHER_VIEWS = {"overview", "profile", "hours", "groups", "students"}


def _normalize_view(value: Any) -> str:
    key = str(value or "").strip().lower()
    return key if key in TEACHER_VIEWS else "overview"


def _teacher_view_href(view: str, *, teacher_id: str = "") -> str:
    params = []
    normalized = _normalize_view(view)
    if normalized != "overview":
        params.append(("view", normalized))
    teacher_key = str(teacher_id or "").strip()
    if teacher_key:
        params.append(("teacher_id", teacher_key))
    if not params:
        return "/teacher"
    return f"/teacher?{urlencode(params, doseq=True)}"


def _redirect_teacher(view: str = "overview", *, teacher_id: str = "", **params: str) -> RedirectResponse:
    location = _teacher_view_href(view, teacher_id=teacher_id)
    extra = [(key, value) for key, value in params.items() if str(value or "").strip()]
    if extra:
        separator = "&" if "?" in location else "?"
        location = f"{location}{separator}{urlencode(extra)}"
    return RedirectResponse(location, status_code=HTTP_302_FOUND)


def _teacher_record_from_request(request: Request, user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    teacher_id = ""
    if is_admin_user(user):
        teacher_id = str(request.query_params.get("teacher_id") or "").strip()
    access_entry = get_teacher_access_entry(user)
    return resolve_teacher_record(teacher_id=teacher_id, access_entry=access_entry)


def _teacher_record_from_form(form: Dict[str, Any], user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    teacher_id = ""
    if is_admin_user(user):
        teacher_id = str(form.get("teacher_id") or "").strip()
    access_entry = get_teacher_access_entry(user)
    return resolve_teacher_record(teacher_id=teacher_id, access_entry=access_entry)


def _can_manage_teacher_record(user: Optional[Dict[str, Any]], teacher_record: Optional[Dict[str, Any]]) -> bool:
    if not user or not teacher_record:
        return False
    if is_admin_user(user):
        return True
    access_entry = get_teacher_access_entry(user)
    resolved = resolve_teacher_record(access_entry=access_entry)
    return bool(resolved and resolved["id"] == teacher_record["id"])


def _normalize_half_hour_time(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw or not re.match(r"^\d{2}:\d{2}$", raw):
        return ""
    hour = int(raw[:2])
    minute = int(raw[3:5])
    if 0 <= hour <= 23 and minute in {0, 30}:
        return raw
    return ""


def _time_value_key(value: str) -> Tuple[int, int]:
    return int(value[:2]), int(value[3:5])


def _parse_availability_form(form: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
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
            start = _normalize_half_hour_time(start_raw)
            end = _normalize_half_hour_time(end_raw)
            if not start or not end:
                return {}, f"{day['label']}: используйте время в формате ЧЧ:ММ с шагом 30 минут."
            if _time_value_key(start) >= _time_value_key(end):
                return {}, f"{day['label']}: начало интервала {slot_index + 1} должно быть раньше окончания."
            slots.append({"start": start, "end": end})
        ordered = sorted(slots, key=lambda item: (_time_value_key(item["start"]), _time_value_key(item["end"])))
        for idx in range(1, len(ordered)):
            if _time_value_key(ordered[idx]["start"]) < _time_value_key(ordered[idx - 1]["end"]):
                return {}, f"{day['label']}: интервалы не должны пересекаться."
        days[day["key"]] = ordered
    return days, ""


def _teacher_page_context(request: Request, *, user: Dict[str, Any]) -> Dict[str, Any]:
    view = _normalize_view(request.query_params.get("view"))
    agreements = load_agreements()
    teacher_record = _teacher_record_from_request(request, user)
    teacher_dashboard = build_teacher_dashboard(teacher_record, agreements)
    teacher_id = teacher_record["id"] if teacher_record else str(request.query_params.get("teacher_id") or "").strip()
    teacher_options = load_teachers() if is_admin_user(user) else ([teacher_record] if teacher_record else [])
    group_edit = None
    group_id = str(request.query_params.get("group_id") or "").strip()
    if group_id and teacher_record:
        candidate = get_study_group(group_id)
        if candidate and candidate["teacher_id"] == teacher_record["id"]:
            group_edit = candidate
    teacher_links = {key: _teacher_view_href(key, teacher_id=teacher_id) for key in TEACHER_VIEWS}
    teacher_message_map = {
        "profile": "Профиль преподавателя обновлён.",
        "hours": "Рабочие часы сохранены.",
        "group": "Группа сохранена.",
        "member_added": "Ученик добавлен в группу.",
        "member_removed": "Ученик убран из группы.",
    }
    return {
        "view": view,
        "teacher_record": teacher_record,
        "teacher_options": teacher_options,
        "teacher_links": teacher_links,
        "teacher_dashboard": teacher_dashboard,
        "teacher_message": teacher_message_map.get(str(request.query_params.get("saved") or "").strip(), ""),
        "teacher_error": str(request.query_params.get("error") or "").strip(),
        "teacher_group_edit": group_edit,
        "teacher_public_profile_url": f"/teachers/{teacher_record['id']}" if teacher_record else "",
        "teacher_selected_id": teacher_id,
        "teacher_view_label": {
            "overview": "Обзор",
            "profile": "Профиль",
            "hours": "Рабочие часы",
            "groups": "Группы",
            "students": "Ученики",
        }.get(view, "Обзор"),
        "teacher_weekdays": WEEKDAY_META,
        "teacher_group_status_meta": GROUP_STATUS_META,
        "teacher_candidate_students": build_candidate_students(
            agreements,
            direction_key=teacher_record.get("speciality", "") if teacher_record else "",
        ),
    }


@router.get("/teacher", include_in_schema=False)
async def teacher_dashboard_page(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    return render(request, "teacher.html", _teacher_page_context(request, user=user))


@router.post("/teacher/profile/save", include_in_schema=False)
async def teacher_profile_save(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    form = await request.form()
    teacher_record = _teacher_record_from_form(form, user)
    if not teacher_record:
        return _redirect_teacher("profile", error="Сначала привяжите аккаунт к карточке преподавателя.")
    name = str(form.get("name") or "").strip() or teacher_record["name"]
    try:
        upsert_teacher_record(
            teacher_id=teacher_record["id"],
            name=name,
            role=str(form.get("role") or "").strip() or teacher_record.get("role", ""),
            bio=str(form.get("bio") or "").strip(),
            speciality=str(form.get("speciality") or "").strip(),
            telegram=str(form.get("telegram") or "").strip(),
            email=str(form.get("email") or "").strip(),
            expertise=str(form.get("expertise") or "").strip(),
            status=teacher_record.get("status", "active"),
            accent=teacher_record.get("accent", "violet"),
        )
    except ValueError as exc:
        return _redirect_teacher("profile", teacher_id=teacher_record["id"], error=str(exc))
    return _redirect_teacher("profile", teacher_id=teacher_record["id"], saved="profile")


@router.post("/teacher/hours/save", include_in_schema=False)
async def teacher_hours_save(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    form = await request.form()
    teacher_record = _teacher_record_from_form(form, user)
    if not teacher_record:
        return _redirect_teacher("hours", error="Не удалось определить преподавателя для сохранения часов.")
    days, error = _parse_availability_form(form)
    if error:
        return _redirect_teacher("hours", teacher_id=teacher_record["id"], error=error)
    save_teacher_availability_record(teacher_record["id"], days)
    return _redirect_teacher("hours", teacher_id=teacher_record["id"], saved="hours")


@router.post("/teacher/groups/save", include_in_schema=False)
async def teacher_group_save(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    form = await request.form()
    teacher_record = _teacher_record_from_form(form, user)
    if not teacher_record:
        return _redirect_teacher("groups", error="Не удалось определить преподавателя для сохранения группы.")
    lesson_duration_minutes = 90
    try:
        lesson_duration_minutes = int(form.get("lesson_duration_minutes") or 90)
    except Exception:
        lesson_duration_minutes = 90
    try:
        upsert_study_group(
            group_id=str(form.get("group_id") or "").strip(),
            teacher_id=teacher_record["id"],
            title=str(form.get("title") or "").strip(),
            direction_key=str(form.get("direction_key") or "").strip(),
            lesson_duration_minutes=lesson_duration_minutes,
            status=str(form.get("status") or "").strip() or "forming",
            final_slot=str(form.get("final_slot") or "").strip(),
        )
    except ValueError as exc:
        return _redirect_teacher("groups", teacher_id=teacher_record["id"], error=str(exc))
    return _redirect_teacher("groups", teacher_id=teacher_record["id"], saved="group")


@router.post("/teacher/groups/member/add", include_in_schema=False)
async def teacher_group_member_add(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    form = await request.form()
    teacher_record = _teacher_record_from_form(form, user)
    group_id = str(form.get("group_id") or "").strip()
    agreement_file = str(form.get("agreement_file") or "").strip()
    group = get_study_group(group_id)
    if not group or not teacher_record or group["teacher_id"] != teacher_record["id"]:
        return _redirect_teacher("groups", teacher_id=teacher_record["id"] if teacher_record else "", error="Группа не найдена.")
    occupied_member_ids = {
        member_id
        for item in load_study_groups()
        if item["id"] != group_id
        for member_id in item.get("member_ids") or []
    }
    candidate_ids = {
        item["agreement_file"]
        for item in build_candidate_students(
            load_agreements(),
            direction_key=group["direction_key"],
            exclude_member_ids=occupied_member_ids,
        )
    }
    allowed_ids = candidate_ids | set(group.get("member_ids") or [])
    if not agreement_file or agreement_file not in allowed_ids:
        return _redirect_teacher("groups", teacher_id=teacher_record["id"], error="Р­С‚РѕРіРѕ СѓС‡РµРЅРёРєР° РЅРµР»СЊР·СЏ РґРѕР±Р°РІРёС‚СЊ РІ РіСЂСѓРїРїСѓ.")
    add_group_member(group_id, agreement_file)
    return _redirect_teacher("groups", teacher_id=teacher_record["id"], saved="member_added")


@router.post("/teacher/groups/member/remove", include_in_schema=False)
async def teacher_group_member_remove(request: Request):
    guard = teacher_required(request)
    if guard:
        return guard
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/teacher", status_code=HTTP_302_FOUND)
    form = await request.form()
    teacher_record = _teacher_record_from_form(form, user)
    group_id = str(form.get("group_id") or "").strip()
    group = get_study_group(group_id)
    if not group or not teacher_record or group["teacher_id"] != teacher_record["id"]:
        return _redirect_teacher("groups", teacher_id=teacher_record["id"] if teacher_record else "", error="Группа не найдена.")
    remove_group_member(group_id, str(form.get("agreement_file") or "").strip())
    return _redirect_teacher("groups", teacher_id=teacher_record["id"], saved="member_removed")


@router.get("/teachers/{teacher_id}", include_in_schema=False)
async def teacher_public_profile_page(request: Request, teacher_id: str):
    teacher_record = get_teacher_record(teacher_id)
    if not teacher_record:
        response = render(request, "404.html")
        response.status_code = 404
        return response
    user = get_current_user(request)
    public_profile = build_teacher_public_profile(teacher_record, load_agreements())
    teacher_manage_href = ""
    if _can_manage_teacher_record(user, teacher_record):
        teacher_manage_href = _teacher_view_href("overview", teacher_id=teacher_record["id"] if is_admin_user(user) else "")
    return render(
        request,
        "teacher_public_profile.html",
        {
            "teacher": public_profile,
            "teacher_manage_href": teacher_manage_href,
        },
    )
