import csv
import io
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

import core
from core import (
    USERS_FILE,
    admin_required,
    filter_items,
    get_admin_ids,
    load_agreements,
    load_json,
    load_leads,
    load_metrics,
    parse_date,
    render,
    save_whitelist,
    update_lead_status,
)

router = APIRouter()

STATUS_META = {
    "new": ("Новая", "status-new"),
    "in_progress": ("В работе", "status-warm"),
    "closed": ("Закрыта", "status-muted"),
    "archived": ("Архив", "status-muted"),
}

STATUS_OPTIONS = [
    ("auto", "Авто"),
    ("new", "Новая"),
    ("in_progress", "В работе"),
    ("closed", "Закрыта"),
]


def format_ts(value: Optional[int]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"


def status_from_item(item: Dict[str, Any]) -> Tuple[str, str, str]:
    manual = (item.get("status") or "").strip()
    if manual in STATUS_META:
        label, cls = STATUS_META[manual]
        return manual, label, cls

    ts = item.get("timestamp")
    if not ts:
        return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]
    try:
        delta = datetime.now() - datetime.fromtimestamp(int(ts))
    except Exception:
        return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]

    if delta <= timedelta(days=1):
        return "new", STATUS_META["new"][0], STATUS_META["new"][1]
    if delta <= timedelta(days=7):
        return "in_progress", STATUS_META["in_progress"][0], STATUS_META["in_progress"][1]
    return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]


def matches_query(item: Dict[str, Any], fields: List[str], query: str) -> bool:
    if not query:
        return True
    value = query.lower()
    for field in fields:
        raw = item.get(field)
        if raw is None:
            continue
        if value in str(raw).lower():
            return True
    return False


def apply_search(items: List[Dict[str, Any]], query: str, fields: List[str]) -> List[Dict[str, Any]]:
    if not query:
        return items
    return [item for item in items if matches_query(item, fields, query)]


def sort_items(items: List[Dict[str, Any]], sort_key: str, order: str, key_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    reverse = order != "asc"
    key_fn = key_map.get(sort_key) or key_map.get("date")
    return sorted(items, key=key_fn, reverse=reverse)


def extract_source(page: str) -> str:
    if not page:
        return "Прямой"
    try:
        parsed = urlparse(page)
        params = parse_qs(parsed.query)
        for key in ("utm_source", "source", "utm"):
            if key in params and params[key]:
                return params[key][0][:48]
        host = (parsed.netloc or "").lower()
    except Exception:
        host = ""

    page_lower = page.lower()
    for token, label in (
        ("google", "Google"),
        ("yandex", "Yandex"),
        ("vk.com", "VK"),
        ("vk", "VK"),
        ("t.me", "Telegram"),
        ("telegram", "Telegram"),
        ("youtube", "YouTube"),
        ("instagram", "Instagram"),
    ):
        if token in page_lower or token in host:
            return label

    if host:
        return host
    return "Прямой"


def build_query(params: Dict[str, str], *, exclude: Optional[List[str]] = None) -> str:
    if exclude:
        for key in exclude:
            params.pop(key, None)
    if not params:
        return ""
    return f"?{urlencode(params)}"


def bucket_counts(items: List[Dict[str, Any]], bucket: str, periods: int) -> List[Dict[str, Any]]:
    today = date.today()
    results = []
    if bucket == "week":
        current_start = today - timedelta(days=today.weekday())
        weeks = [current_start - timedelta(days=7 * i) for i in range(periods - 1, -1, -1)]
        counts = {start: 0 for start in weeks}
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                d = datetime.fromtimestamp(int(ts)).date()
            except Exception:
                continue
            start = d - timedelta(days=d.weekday())
            if start in counts:
                counts[start] += 1
        max_count = max(counts.values()) if counts else 1
        for start in weeks:
            label = f"{start.strftime('%d.%m')}"
            results.append({"label": label, "count": counts[start], "pct": round((counts[start] / max_count) * 100, 1)})
        return results

    if bucket == "month":
        months = []
        year = today.year
        month = today.month
        for _ in range(periods):
            months.append((year, month))
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        months = list(reversed(months))
        counts = {m: 0 for m in months}
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                d = datetime.fromtimestamp(int(ts)).date()
            except Exception:
                continue
            key = (d.year, d.month)
            if key in counts:
                counts[key] += 1
        max_count = max(counts.values()) if counts else 1
        for year, month in months:
            label = f"{month:02d}.{str(year)[-2:]}"
            results.append({"label": label, "count": counts[(year, month)], "pct": round((counts[(year, month)] / max_count) * 100, 1)})
        return results
    return results


@router.get("/admin", include_in_schema=False)
def admin_panel(request: Request):
    guard = admin_required(request)
    if guard:
        return guard

    metrics = load_metrics()
    leads_all = load_leads()
    agreements_all = load_agreements()
    users_data = load_json(USERS_FILE, {})
    if not isinstance(users_data, dict):
        users_data = {}

    course = request.query_params.get("course") or ""
    date_from_value = request.query_params.get("date_from", "")
    date_to_value = request.query_params.get("date_to", "")
    query = (request.query_params.get("q") or "").strip()
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"
    limit_raw = request.query_params.get("limit") or "20"
    date_from = parse_date(date_from_value)
    date_to = parse_date(date_to_value)

    leads = filter_items(leads_all, course, date_from, date_to)
    agreements = filter_items(agreements_all, course, date_from, date_to)

    leads = apply_search(leads, query, ["name", "contact", "course", "page"])
    agreements = apply_search(agreements, query, ["full_name", "phone", "email", "telegram", "course"])

    def lead_sort_key(item: Dict[str, Any]):
        status_key, _, _ = status_from_item(item)
        order_map = {"new": 0, "in_progress": 1, "closed": 2, "archived": 3}
        return (order_map.get(status_key, 3), int(item.get("timestamp", 0)))

    lead_key_map = {
        "date": lambda item: int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": lead_sort_key,
    }
    agreement_key_map = {
        "date": lambda item: int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("full_name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
    }

    leads = sort_items(leads, sort, order, lead_key_map)
    agreements = sort_items(agreements, sort, order, agreement_key_map)

    limit_value = None
    if limit_raw and limit_raw.lower() not in {"all", "0"}:
        try:
            limit_value = max(int(limit_raw), 1)
        except Exception:
            limit_value = 20

    leads_display = leads if limit_value is None else leads[:limit_value]
    agreements_display = agreements if limit_value is None else agreements[:limit_value]

    courses = sorted(
        {item.get("course") for item in (leads_all + agreements_all) if item.get("course")}
    )

    leads_total = len(leads_all)
    agreements_total = len(agreements_all)
    users_total = len(users_data)
    leads_count = len(leads)
    agreements_count = len(agreements)

    leads_view = []
    for item in leads_display:
        status_key, status_label, status_class = status_from_item(item)
        manual_status = (item.get("status") or "").strip()
        leads_view.append(
            {
                **item,
                "display_time": format_ts(item.get("timestamp")),
                "status_label": status_label,
                "status_class": status_class,
                "status_key": status_key,
                "manual_status": manual_status,
                "source": extract_source(item.get("page", "")),
            }
        )

    agreements_view = []
    for item in agreements_display:
        agreements_view.append(
            {
                **item,
                "display_time": format_ts(item.get("timestamp")),
                "status_label": "Подписан",
                "status_class": "status-good",
            }
        )

    users_view = []
    for user in users_data.values():
        if not isinstance(user, dict):
            continue
        users_view.append(
            {
                "id": user.get("id"),
                "provider": user.get("provider", "—"),
                "email": user.get("email") or "—",
                "name": user.get("name") or user.get("email") or user.get("id") or "—",
            }
        )
    users_view.sort(key=lambda item: (str(item.get("provider")), str(item.get("name"))))

    filter_bits = []
    if course:
        filter_bits.append(f"Курс: {course}")
    if query:
        filter_bits.append(f"Поиск: {query}")
    if date_from_value or date_to_value:
        def fmt_date(value: str) -> str:
            if not value:
                return "…"
            try:
                return datetime.strptime(value, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                return value
        filter_bits.append(f"Период: {fmt_date(date_from_value)} — {fmt_date(date_to_value)}")
    filters_label = " · ".join(filter_bits) if filter_bits else "Все данные"

    params = {}
    if course:
        params["course"] = course
    if date_from_value:
        params["date_from"] = date_from_value
    if date_to_value:
        params["date_to"] = date_to_value
    if query:
        params["q"] = query
    if sort:
        params["sort"] = sort
    if order:
        params["order"] = order
    if limit_raw:
        params["limit"] = limit_raw

    filters_query = build_query(dict(params))
    export_query = build_query(dict(params), exclude=["limit"])

    path_counts = metrics.get("path_counts", {})
    path_counts_sorted = sorted(path_counts.items(), key=lambda item: item[1], reverse=True)

    def pct(part: int, total: int) -> float:
        if not total:
            return 0.0
        return round((part / total) * 100, 1)

    funnel = metrics.get("funnel", {})
    funnel_home = int(funnel.get("home", 0) or 0)
    funnel_login = int(funnel.get("login", 0) or 0)
    funnel_apply = int(funnel.get("apply", 0) or 0)
    funnel_enroll = int(funnel.get("enroll", 0) or 0)
    funnel_max = max(funnel_home, funnel_login, funnel_apply, funnel_enroll, 1)
    funnel_steps = [
        {
            "label": "Главная",
            "count": funnel_home,
            "pct": pct(funnel_home, funnel_max),
            "rate": "100%",
        },
        {
            "label": "Логин",
            "count": funnel_login,
            "pct": pct(funnel_login, funnel_max),
            "rate": f"{pct(funnel_login, funnel_home)}%",
        },
        {
            "label": "Заявки",
            "count": funnel_apply,
            "pct": pct(funnel_apply, funnel_max),
            "rate": f"{pct(funnel_apply, funnel_login)}%",
        },
        {
            "label": "Покупки",
            "count": funnel_enroll,
            "pct": pct(funnel_enroll, funnel_max),
            "rate": f"{pct(funnel_enroll, funnel_apply)}%",
        },
    ]

    today = date.today()
    days = [today - timedelta(days=offset) for offset in range(6, -1, -1)]
    lead_counts = {d: 0 for d in days}
    enroll_counts = {d: 0 for d in days}
    for item in leads_all:
        ts = item.get("timestamp")
        if ts:
            d = datetime.fromtimestamp(int(ts)).date()
            if d in lead_counts:
                lead_counts[d] += 1
    for item in agreements_all:
        ts = item.get("timestamp")
        if ts:
            d = datetime.fromtimestamp(int(ts)).date()
            if d in enroll_counts:
                enroll_counts[d] += 1
    lead_max = max(lead_counts.values()) if lead_counts else 1
    enroll_max = max(enroll_counts.values()) if enroll_counts else 1
    lead_chart = [
        {"label": d.strftime("%d.%m"), "count": lead_counts[d], "pct": pct(lead_counts[d], lead_max)}
        for d in days
    ]
    enroll_chart = [
        {"label": d.strftime("%d.%m"), "count": enroll_counts[d], "pct": pct(enroll_counts[d], enroll_max)}
        for d in days
    ]

    weekly_leads = bucket_counts(leads_all, "week", 8)
    weekly_enrolls = bucket_counts(agreements_all, "week", 8)
    monthly_leads = bucket_counts(leads_all, "month", 6)
    monthly_enrolls = bucket_counts(agreements_all, "month", 6)

    last_ts = 0
    for item in (leads_all + agreements_all):
        ts = item.get("timestamp")
        if ts and int(ts) > last_ts:
            last_ts = int(ts)
    last_activity = format_ts(last_ts) if last_ts else "—"

    def count_recent(items: List[Dict[str, Any]], hours: int) -> int:
        if not items:
            return 0
        since = datetime.now() - timedelta(hours=hours)
        total = 0
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                if datetime.fromtimestamp(int(ts)) >= since:
                    total += 1
            except Exception:
                continue
        return total

    leads_24h = count_recent(leads_all, 24)
    enroll_24h = count_recent(agreements_all, 24)
    leads_7d = sum(lead_counts.values())
    enroll_7d = sum(enroll_counts.values())

    kpis = [
        {
            "label": "Посещения",
            "value": metrics.get("total_visits", 0),
            "note": "Все визиты сайта",
        },
        {
            "label": "Уникальные",
            "value": metrics.get("unique_visits", 0),
            "note": "Сессии пользователей",
        },
        {
            "label": "Заявки",
            "value": funnel_apply,
            "note": f"За 24ч: {leads_24h} · 7 дней: {leads_7d}",
        },
        {
            "label": "Покупки",
            "value": funnel_enroll,
            "note": f"За 24ч: {enroll_24h} · 7 дней: {enroll_7d}",
        },
        {
            "label": "Конверсия в заявку",
            "value": f"{pct(funnel_apply, metrics.get('unique_visits', 0))}%",
            "note": "От уникальных визитов",
        },
        {
            "label": "Конверсия в покупку",
            "value": f"{pct(funnel_enroll, funnel_apply)}%",
            "note": "От заявок",
        },
    ]

    course_counts = {}
    agreement_counts = {}
    for item in leads_all:
        course_name = item.get("course")
        if course_name:
            course_counts[course_name] = course_counts.get(course_name, 0) + 1
    for item in agreements_all:
        course_name = item.get("course")
        if course_name:
            agreement_counts[course_name] = agreement_counts.get(course_name, 0) + 1
    top_courses = sorted(course_counts.items(), key=lambda item: item[1], reverse=True)[:6]
    top_agreements = sorted(agreement_counts.items(), key=lambda item: item[1], reverse=True)[:6]

    source_counts = {}
    for item in leads_all:
        source = extract_source(item.get("page", ""))
        source_counts[source] = source_counts.get(source, 0) + 1
    sources_sorted = sorted(source_counts.items(), key=lambda item: item[1], reverse=True)[:6]

    recent_leads = []
    for item in leads_all[:6]:
        _, label, cls = status_from_item(item)
        recent_leads.append(
            {
                "name": item.get("name") or "Без имени",
                "time": format_ts(item.get("timestamp")),
                "course": item.get("course") or "—",
                "status_label": label,
                "status_class": cls,
            }
        )

    next_url = f"/admin{request.url.query and ('?' + request.url.query) or ''}"

    return render(
        request,
        "admin.html",
        {
            "metrics": metrics,
            "leads": leads_view,
            "agreements": agreements_view,
            "users": users_view,
            "whitelist": core.WHITELIST_IDS,
            "admin_ids": get_admin_ids(),
            "path_counts_sorted": path_counts_sorted,
            "user": request.session.get("user"),
            "courses": courses,
            "filters": {
                "course": course,
                "date_from": date_from_value,
                "date_to": date_to_value,
                "query": query,
                "sort": sort,
                "order": order,
                "limit": limit_raw,
            },
            "lead_chart": lead_chart,
            "enroll_chart": enroll_chart,
            "weekly_leads": weekly_leads,
            "weekly_enrolls": weekly_enrolls,
            "monthly_leads": monthly_leads,
            "monthly_enrolls": monthly_enrolls,
            "funnel_steps": funnel_steps,
            "kpis": kpis,
            "filters_label": filters_label,
            "filters_query": filters_query,
            "export_query": export_query,
            "leads_total": leads_total,
            "agreements_total": agreements_total,
            "users_total": users_total,
            "leads_count": leads_count,
            "agreements_count": agreements_count,
            "last_activity": last_activity,
            "top_courses": top_courses,
            "top_agreements": top_agreements,
            "sources_sorted": sources_sorted,
            "status_options": STATUS_OPTIONS,
            "next_url": next_url,
            "recent_leads": recent_leads,
        },
    )


@router.post("/admin/leads/status", include_in_schema=False)
async def admin_update_lead_status(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    status = str(form.get("status") or "").strip()
    next_url = str(form.get("next") or "/admin")
    if status == "auto":
        status = ""
    update_lead_status(file_name, status)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/whitelist", include_in_schema=False)
async def admin_update_whitelist(request: Request):
    guard = admin_required(request)
    if guard:
        return guard

    form = await request.form()
    raw = str(form.get("whitelist", "")).strip()
    ids = []
    for part in re.split(r"[,\n ]+", raw):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except Exception:
            continue
    if ids:
        core.WHITELIST_IDS = ids
        save_whitelist(ids)
    return RedirectResponse("/admin", status_code=HTTP_302_FOUND)


@router.post("/admin/whitelist/remove", include_in_schema=False)
async def admin_remove_whitelist(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    try:
        target = int(form.get("id"))
    except Exception:
        return RedirectResponse("/admin", status_code=HTTP_302_FOUND)
    ids = [item for item in core.WHITELIST_IDS if item != target]
    if ids:
        core.WHITELIST_IDS = ids
        save_whitelist(ids)
    return RedirectResponse("/admin", status_code=HTTP_302_FOUND)


@router.get("/admin/export/leads.csv", include_in_schema=False)
def export_leads(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    course = request.query_params.get("course") or ""
    date_from = parse_date(request.query_params.get("date_from"))
    date_to = parse_date(request.query_params.get("date_to"))
    query = (request.query_params.get("q") or "").strip()
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"

    leads = filter_items(load_leads(), course, date_from, date_to)
    leads = apply_search(leads, query, ["name", "contact", "course", "page"])

    def lead_sort_key(item: Dict[str, Any]):
        status_key, _, _ = status_from_item(item)
        order_map = {"new": 0, "in_progress": 1, "closed": 2, "archived": 3}
        return (order_map.get(status_key, 3), int(item.get("timestamp", 0)))

    lead_key_map = {
        "date": lambda item: int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": lead_sort_key,
    }
    leads = sort_items(leads, sort, order, lead_key_map)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "name", "contact", "course", "page", "status"])
    for item in leads:
        _, status_label, _ = status_from_item(item)
        writer.writerow([
            item.get("timestamp"),
            item.get("name"),
            item.get("contact"),
            item.get("course"),
            item.get("page"),
            status_label,
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@router.get("/admin/export/agreements.csv", include_in_schema=False)
def export_agreements(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    course = request.query_params.get("course") or ""
    date_from = parse_date(request.query_params.get("date_from"))
    date_to = parse_date(request.query_params.get("date_to"))
    query = (request.query_params.get("q") or "").strip()
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"

    agreements = filter_items(load_agreements(), course, date_from, date_to)
    agreements = apply_search(agreements, query, ["full_name", "phone", "email", "telegram", "course"])

    agreement_key_map = {
        "date": lambda item: int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("full_name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
    }
    agreements = sort_items(agreements, sort, order, agreement_key_map)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "course", "full_name", "phone", "email", "telegram"])
    for item in agreements:
        writer.writerow([
            item.get("timestamp"),
            item.get("course"),
            item.get("full_name"),
            item.get("phone"),
            item.get("email"),
            item.get("telegram"),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=agreements.csv"},
    )


@router.get("/admin/export/users.csv", include_in_schema=False)
def export_users(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "email", "name", "provider"])
    users = load_json(USERS_FILE, {})
    for item in users.values():
        if not isinstance(item, dict):
            continue
        writer.writerow([
            item.get("id"),
            item.get("email"),
            item.get("name"),
            item.get("provider"),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"},
    )
