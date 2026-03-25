from datetime import datetime, timedelta
from typing import Any, Dict, List


ACCOUNT_API_ENDPOINTS = {
    "schedule": "/api/account/schedule",
    "lectures": "/api/account/lectures",
}

ACCOUNT_SECTION_META = {
    "schedule": {
        "kicker": "Schedule",
        "title": "Расписание",
        "empty_title": "Расписание пока пустое",
        "empty_text": "Когда курс и занятия будут назначены, ближайшие события появятся здесь и в mock API.",
    },
    "lectures": {
        "kicker": "Lectures",
        "title": "Мои лекции",
        "empty_title": "Записей лекций пока нет",
        "empty_text": "Раздел уже подготовлен. Когда в backend появится хранение записей, они будут выводиться здесь автоматически.",
    },
}


def _format_schedule_dt(value: datetime) -> str:
    return value.strftime("%d.%m · %H:%M")


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
        "lead": "Здесь собраны договоры, прогресс по курсам, материалы и оплата.",
        "summary": [
            "Договоры и PDF в одном месте",
            "Календарь по каждому курсу",
            "Материалы и оплата без лишних шагов",
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
                    "курсу, оплате и материалам."
                ),
            },
            {
                "kicker": "Оплата",
                "title": "СБП включена" if payments_enabled else "Онлайн-оплата выключена",
                "text": (
                    "Можно оплачивать занятия прямо из карточки курса и продолжать незавершённый "
                    "платёж без повторного ввода данных."
                    if payments_enabled
                    else "Сейчас доступны только договоры, календарь и материалы. Онлайн-оплата появится позже."
                ),
            },
            {
                "kicker": "Материалы и встречи",
                "title": "Доступ открывается по активным курсам" if has_agreements else "Доступ откроется после записи",
                "text": (
                    "Внутри каждой карточки есть календарь месяца, модульные материалы и статусы "
                    "по договору, чтобы не искать это в разных местах."
                ),
            },
        ],
    }


def build_account_schedule_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not agreements:
        return []

    now = datetime.now()
    schedule = []
    base_offsets = [
        ("Ближайшая лекция", 1, 19, 0, "Онлайн", "live"),
        ("Практика по модулю", 3, 20, 0, "Zoom", "practice"),
        ("Разбор домашнего задания", 5, 18, 30, "Google Meet", "review"),
    ]
    for idx, item in enumerate(agreements[:3]):
        title, days, hour, minute, channel, kind = base_offsets[idx % len(base_offsets)]
        starts_at = (now + timedelta(days=days)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        schedule.append(
            {
                "id": f"mock-{idx + 1}",
                "course": item.get("course") or "Курс",
                "title": title,
                "starts_at": starts_at.isoformat(),
                "starts_label": _format_schedule_dt(starts_at),
                "duration_label": "90 минут",
                "channel": channel,
                "teacher": "Преподаватель будет назначен",
                "kind": kind,
                "status": "planned",
            }
        )
    return schedule


def build_account_lectures_mock(agreements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return []


def build_account_mock_content(
    agreements: List[Dict[str, Any]],
    *,
    user_display: str,
    payments_enabled: bool,
) -> Dict[str, Any]:
    schedule = build_account_schedule_mock(agreements)
    lectures = build_account_lectures_mock(agreements)
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
