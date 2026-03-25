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


def build_account_mock_content(agreements: List[Dict[str, Any]]) -> Dict[str, Any]:
    schedule = build_account_schedule_mock(agreements)
    lectures = build_account_lectures_mock(agreements)
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
        "sections": sections,
        "stats": {
            "upcoming_events": len(schedule),
            "lecture_records": len(lectures),
        },
    }
