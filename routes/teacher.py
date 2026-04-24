from typing import Any, Dict, Optional
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


def _teacher_page_context(request: Request, *, user: Dict[str, Any]) -> Dict[str, Any]:
    view = _normalize_view(request.query_params.get("view"))
    agreements = load_agreements()
    teacher_record = _teacher_record_from_request(request, user)
    teacher_dashboard = build_teacher_dashboard(teacher_record, agreements)
    teacher_id = teacher_record["id"] if teacher_record else str(request.query_params.get("teacher_id") or "").strip()
    teacher_options = load_teachers() if is_admin_user(user) else ([teacher_record] if teacher_record else [])
    group_edit = None
    group_id = str(request.query_params.get("group_id") or "").strip()
    if group_id:
        candidate = get_study_group(group_id)
        if candidate and teacher_record and candidate["teacher_id"] == teacher_record["id"]:
            group_edit = candidate
        elif candidate and is_admin_user(user) and teacher_record and candidate["teacher_id"] == teacher_record["id"]:
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


def _parse_availability_form(form: Dict[str, Any]) -> Dict[str, Any]:
    days = {}
    for day in WEEKDAY_META:
        slots = []
        for slot_index in range(2):
            start = str(form.get(f"{day['key']}_start_{slot_index}") or "").strip()
            end = str(form.get(f"{day['key']}_end_{slot_index}") or "").strip()
            if start and end:
                slots.append({"start": start, "end": end})
        days[day["key"]] = slots
    return days


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
    save_teacher_availability_record(teacher_record["id"], _parse_availability_form(form))
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
    group = get_study_group(group_id)
    if not group or not teacher_record or group["teacher_id"] != teacher_record["id"]:
        return _redirect_teacher("groups", teacher_id=teacher_record["id"] if teacher_record else "", error="Группа не найдена.")
    add_group_member(group_id, str(form.get("agreement_file") or "").strip())
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
    public_profile = build_teacher_public_profile(teacher_record, load_agreements())
    return render(
        request,
        "teacher_public_profile.html",
        {
            "teacher": public_profile,
        },
    )
