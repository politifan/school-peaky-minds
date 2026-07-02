import calendar
import json
import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


BASE_DIR = Path(__file__).resolve().parent.parent
MARKETING_CONFIG_FILE = BASE_DIR / "config" / "marketing_config.json"
MOSCOW_TZ = timezone(timedelta(hours=3))


DEFAULT_MARKETING_RUNTIME: Dict[str, Any] = {
    "promo": {
        "label": "Актуальное предложение",
        "discount": "-20%",
        "deadline_iso": "2026-05-01T23:59:59+03:00",
        "deadline_label": "",
        "seats_left": 3,
        "summary": "Фиксируем место в мини-группе и текущую цену до конца акции.",
    },
    "sticky_bar": {
        "title": "Сомневаешься? Задай вопрос в Telegram за 30 секунд.",
        "button": "Открыть Telegram",
    },
    "video": {
        "enabled": True,
        "title": "Как выглядит обучение изнутри",
        "eyebrow": "Формат без витрины",
        "description": "Показываем ритм занятий, практику и сопровождение так, чтобы было понятно, как вы двигаетесь от первой задачи к портфолио.",
        "duration": "45-60 секунд",
        "video_url": "",
        "poster": "/assets/img/logo.png",
    },
}


def _next_month_end_deadline(now: datetime | None = None) -> tuple[str, str]:
    current = now or datetime.now(MOSCOW_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=MOSCOW_TZ)
    current = current.astimezone(MOSCOW_TZ)

    year = current.year
    month = current.month
    last_day = calendar.monthrange(year, month)[1]
    deadline = datetime(year, month, last_day, 23, 59, 59, tzinfo=MOSCOW_TZ)

    if deadline <= current:
        month += 1
        if month > 12:
            year += 1
            month = 1
        last_day = calendar.monthrange(year, month)[1]
        deadline = datetime(year, month, last_day, 23, 59, 59, tzinfo=MOSCOW_TZ)

    return deadline.isoformat(), f"\u0434\u043e {deadline.day}.{deadline.month:02d}"


def _deadline_is_future(value: Any) -> bool:
    try:
        deadline = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return False
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=MOSCOW_TZ)
    return deadline > datetime.now(MOSCOW_TZ)


def _refresh_offer_deadline(offer: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(offer, dict):
        return offer
    if _deadline_is_future(offer.get("deadline_iso")):
        return offer
    deadline_iso, deadline_label = _next_month_end_deadline()
    offer["deadline_iso"] = deadline_iso
    offer["deadline_label"] = deadline_label
    return offer


TRACK_MARKETING: Dict[str, Dict[str, Any]] = {
    "one_c_developer": {
        "category": "business",
        "slogan": "Практичный вход в корпоративную разработку через 1C и реальные процессы компаний.",
        "summary": "Конфигурации, отчеты, доработки и обмены как понятный маршрут в прикладную разработку.",
        "search_tags": ["1c", "1с", "1c разработчик", "корпоративные системы", "учет", "автоматизация учета"],
        "salary": {
            "range": "90 000 - 160 000 ₽",
            "label": "Ориентир для junior 1C-разработчика",
        },
        "roi": {
            "course_cost": 70000,
            "time_to_offer": 5,
            "entry_salary": 100000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Дмитрий",
            "result": "первый корпоративный кейс по 1C и уверенный вход в собеседования",
            "timeline": "5-6 месяцев до первых 1C-интервью",
            "href": "/blog",
        },
    },
    "product_manager": {
        "category": "business",
        "slogan": "Продуктовое мышление, которое превращает идеи в релизы, метрики и рост.",
        "summary": "Исследования, roadmap, MVP и коммуникация с командой без хаоса и воды.",
        "search_tags": ["product manager", "продакт", "jtbd", "custdev", "mvp", "roadmap"],
        "salary": {
            "range": "90 000 - 170 000 ₽",
            "label": "Ориентир для junior product manager",
        },
        "roi": {
            "course_cost": 64000,
            "time_to_offer": 5,
            "entry_salary": 100000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Юлия",
            "result": "MVP-кейс, метрики и первые product-интервью",
            "timeline": "5 месяцев до первых product-интервью",
            "href": "/blog",
        },
    },
    "python_start": {
        "category": "start",
        "slogan": "Быстрый старт в коде без перегруза теорией.",
        "summary": "Первый рабочий Python, первые мини-кейсы и понятный следующий шаг.",
        "search_tags": ["с нуля", "детям", "подросткам", "python start", "первый курс", "новичкам"],
        "salary": {
            "range": "55 000 - 90 000 ₽",
            "label": "Ориентир после старта и первых задач",
        },
        "roi": {
            "course_cost": 36000,
            "time_to_offer": 3,
            "entry_salary": 65000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 5,
        },
        "alumni": {
            "name": "Полина",
            "result": "первый Telegram-бот и первый заказ",
            "timeline": "3 месяца до первого оплачиваемого кейса",
            "href": "/posts/first-freelance-money",
        },
    },
    "frontend": {
        "category": "development",
        "slogan": "Интерфейсы, которые не стыдно показать в портфолио.",
        "summary": "HTML, CSS, JS и продуктовая логика без бессмысленных макетов.",
        "search_tags": ["frontend", "ui", "верстка", "интерфейсы", "веб", "сайты"],
        "salary": {
            "range": "80 000 - 140 000 ₽",
            "label": "Ориентир для junior front-end",
        },
        "roi": {
            "course_cost": 68000,
            "time_to_offer": 5,
            "entry_salary": 95000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Ирина",
            "result": "лендинг и UI-кейс для портфолио",
            "timeline": "4-5 месяцев до первых откликов",
            "href": "/blog",
        },
    },
    "backend": {
        "category": "development",
        "slogan": "Backend, который решает задачи бизнеса, а не только учебника.",
        "summary": "FastAPI, базы, архитектура и production-мышление в одном маршруте.",
        "search_tags": ["backend", "api", "fastapi", "python backend", "сервер", "база данных"],
        "salary": {
            "range": "90 000 - 160 000 ₽",
            "label": "Ориентир для junior backend",
        },
        "roi": {
            "course_cost": 76000,
            "time_to_offer": 6,
            "entry_salary": 110000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 3,
        },
        "alumni": {
            "name": "Никита",
            "result": "backend-проект и серия техсобесов",
            "timeline": "6 месяцев до первых офферов",
            "href": "/posts/from-office-to-it-story",
        },
    },
    "fullstack": {
        "category": "development",
        "slogan": "Только суть. Никакой воды. Только код и кейсы.",
        "summary": "Полный маршрут от Python-базы до API, интерфейса и собеседований.",
        "search_tags": ["fullstack", "full stack", "смена профессии", "взрослым", "веб разработка"],
        "salary": {
            "range": "100 000 - 170 000 ₽",
            "label": "Ориентир для junior full-stack",
        },
        "roi": {
            "course_cost": 82000,
            "time_to_offer": 6,
            "entry_salary": 120000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 3,
        },
        "alumni": {
            "name": "Даниил",
            "result": "pet-проект, тестовые и техсобесы",
            "timeline": "6-7 месяцев до сильных интервью",
            "href": "/posts/from-office-to-it-story",
        },
    },
    "qa_engineer": {
        "category": "quality",
        "slogan": "Качество продукта начинается с системного мышления.",
        "summary": "Тест-дизайн, API, SQL и automation как реальный junior-маршрут.",
        "search_tags": ["qa", "тестировщик", "api testing", "manual qa", "automation qa"],
        "salary": {
            "range": "70 000 - 130 000 ₽",
            "label": "Ориентир для junior QA",
        },
        "roi": {
            "course_cost": 62000,
            "time_to_offer": 4,
            "entry_salary": 85000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Марина",
            "result": "первый набор API-кейсов и QA-резюме",
            "timeline": "4 месяца до первых откликов",
            "href": "/blog",
        },
    },
    "data_analyst": {
        "category": "analytics",
        "slogan": "Данные без воды: SQL, BI и логика решений.",
        "summary": "Сильная аналитическая база для первой data-роли.",
        "search_tags": ["аналитика", "data analyst", "sql", "дашборды", "таблицы", "данные"],
        "salary": {
            "range": "80 000 - 140 000 ₽",
            "label": "Ориентир для junior data analyst",
        },
        "roi": {
            "course_cost": 64000,
            "time_to_offer": 4,
            "entry_salary": 95000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Анна",
            "result": "SQL-портфолио и интервью-кейсы",
            "timeline": "4-5 месяцев до аналитических собеседований",
            "href": "/posts/sql-interview-task-breakdown",
        },
    },
    "business_analyst": {
        "category": "analytics",
        "slogan": "Требования, процессы и логика продукта в одной системе.",
        "summary": "Бизнес-аналитика как понятный мост между задачей и решением.",
        "search_tags": ["business analyst", "бизнес аналитик", "требования", "процессы", "uml", "bpmn"],
        "salary": {
            "range": "90 000 - 150 000 ₽",
            "label": "Ориентир для junior business analyst",
        },
        "roi": {
            "course_cost": 70000,
            "time_to_offer": 5,
            "entry_salary": 100000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Елена",
            "result": "первый пакет требований и кейс по процессам",
            "timeline": "5 месяцев до первых BA-интервью",
            "href": "/blog",
        },
    },
    "system_analyst": {
        "category": "analytics",
        "slogan": "Системное мышление вместо хаоса в интеграциях.",
        "summary": "Схемы, API, ERD и архитектурная логика под реальные команды.",
        "search_tags": ["system analyst", "системный аналитик", "api", "erd", "интеграции", "uml"],
        "salary": {
            "range": "95 000 - 160 000 ₽",
            "label": "Ориентир для junior system analyst",
        },
        "roi": {
            "course_cost": 72000,
            "time_to_offer": 5,
            "entry_salary": 105000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Кирилл",
            "result": "системный кейс и документация для интервью",
            "timeline": "5-6 месяцев до первых интервью",
            "href": "/blog",
        },
    },
    "data_science": {
        "category": "analytics",
        "slogan": "Не читай - смотри и зарабатывай.",
        "summary": "NLP, CV и ML-сервисы с понятной дорогой к стажировке.",
        "search_tags": ["ml", "data science", "машинное обучение", "нейросети", "cv", "nlp"],
        "salary": {
            "range": "110 000 - 190 000 ₽",
            "label": "Ориентир для junior ML / DS",
        },
        "roi": {
            "course_cost": 88000,
            "time_to_offer": 7,
            "entry_salary": 125000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 3,
        },
        "alumni": {
            "name": "Алина",
            "result": "NLP-кейс и первый ML-стажировочный трек",
            "timeline": "7-8 месяцев до первых интервью",
            "href": "/posts/sql-interview-task-breakdown",
        },
    },
    "sysadmin": {
        "category": "infrastructure",
        "slogan": "Инфраструктура без хаоса и магии.",
        "summary": "Серверы, сети и базовый прод-контур под системную роль.",
        "search_tags": ["sysadmin", "администратор", "linux", "сети", "серверы", "infra"],
        "salary": {
            "range": "75 000 - 130 000 ₽",
            "label": "Ориентир для junior sysadmin",
        },
        "roi": {
            "course_cost": 60000,
            "time_to_offer": 4,
            "entry_salary": 85000,
        },
        "offer": {
            "discount": "-15%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 4,
        },
        "alumni": {
            "name": "Сергей",
            "result": "первый серверный контур и junior-интервью",
            "timeline": "4-5 месяцев до входа в infra",
            "href": "/blog",
        },
    },
    "devops": {
        "category": "infrastructure",
        "slogan": "Прод без паники: CI/CD, Docker и стабильный контур.",
        "summary": "Маршрут в DevOps через практику, а не через случайный список терминов.",
        "search_tags": ["devops", "docker", "ci cd", "kubernetes", "infra", "облака"],
        "salary": {
            "range": "120 000 - 220 000 ₽",
            "label": "Ориентир для junior DevOps",
        },
        "roi": {
            "course_cost": 92000,
            "time_to_offer": 7,
            "entry_salary": 135000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 3,
        },
        "alumni": {
            "name": "Роман",
            "result": "CI/CD-кейс и инфраструктурное портфолио",
            "timeline": "6-7 месяцев до первых DevOps-интервью",
            "href": "/blog",
        },
    },
    "business": {
        "category": "business",
        "slogan": "Код, который быстро приносит пользу и деньги.",
        "summary": "Боты, парсеры, интеграции и automation-сценарии для бизнеса.",
        "search_tags": ["automation", "бизнес", "telegram", "боты", "парсинг", "быстрый старт"],
        "salary": {
            "range": "85 000 - 150 000 ₽",
            "label": "Ориентир для automation / freelance",
        },
        "roi": {
            "course_cost": 58000,
            "time_to_offer": 3,
            "entry_salary": 90000,
        },
        "offer": {
            "discount": "-20%",
            "deadline_iso": "2026-05-01T23:59:59+03:00",
            "deadline_label": "",
            "seats_left": 3,
        },
        "alumni": {
            "name": "Ольга",
            "result": "бот и automation-кейс для бизнеса",
            "timeline": "2-3 месяца до первых оплачиваемых задач",
            "href": "/posts/first-freelance-money",
        },
    },
}


HOME_MARKETING: Dict[str, Any] = {
    "hero_manifesto": {
        "eyebrow": "Белый экран. Чёрный фокус. Живой результат.",
        "lines": [
            "Не читай - смотри и зарабатывай.",
            "Только суть. Никакой воды. Только код и кейсы.",
            "Белый фон - чтобы ты видел только свою будущую зарплату.",
        ],
        "value": "Мы учим не смотреть уроки, а как можно быстрее дойти до портфолио, денег и реальных задач.",
    },
    "quick_filters": [
        {"label": "С нуля", "group": "start", "search": "с нуля новичкам"},
        {"label": "Смена профессии", "group": "development", "search": "смена профессии fullstack backend"},
        {"label": "Для детей", "group": "start", "search": "детям подросткам python"},
        {"label": "Быстрый старт", "group": "business", "search": "automation telegram боты"},
        {"label": "Высокий потолок", "group": "infrastructure", "search": "devops data science"},
    ],
    "benefits_summary": {
        "title": "Квинтэссенция ценности",
        "text": "Peaky Minds переводит обучение в понятную траекторию: что учить, на чём тренироваться и в какой момент выходить на рынок. Вы не тонете в материалах, а двигаетесь к проектам, деньгам и сильному собеседованию.",
    },
    "it_benefits": [
        {"value": "от 80 000 ₽", "title": "стартовые junior-ориентиры", "text": "по большинству цифровых ролей уже на входном уровне."},
        {"value": "3-8 месяцев", "title": "до первых коммерческих задач", "text": "если держать темп и не выпадать из практики."},
        {"value": "удалёнка + freelance", "title": "несколько сценариев роста", "text": "не только найм, но и проектная работа."},
    ],
    "salary_guide": [
        {"track": "Full-stack / Backend", "range": "100 000 - 170 000 ₽", "note": "junior и junior+"},
        {"track": "Data Science / ML", "range": "110 000 - 190 000 ₽", "note": "после сильного портфолио"},
        {"track": "Automation / Business", "range": "85 000 - 150 000 ₽", "note": "найм и freelance"},
        {"track": "QA / Analytics", "range": "70 000 - 150 000 ₽", "note": "зависит от стекa и задач"},
    ],
    "alumni_cases": [
        {
            "name": "Илья",
            "track": "Backend track",
            "result": "из офиса в backend-практику за 8 месяцев",
            "company": "первые технические интервью",
            "href": "/posts/from-office-to-it-story",
        },
        {
            "name": "Студент automation-track",
            "track": "Automation track",
            "result": "первые деньги на фрилансе после второго модуля",
            "company": "реальный заказ, а не учебный кейс",
            "href": "/posts/first-freelance-money",
        },
        {
            "name": "Анна",
            "track": "Data / SQL track",
            "result": "SQL-кейсы и уверенность на аналитических интервью",
            "company": "сильная база под junior data",
            "href": "/posts/sql-interview-task-breakdown",
        },
    ],
    "audience_spotlight": {
        "eyebrow": "Форматы",
        "title": "Отдельный маршрут для взрослых и для детей",
        "description": "Взрослым важны скорость, рынок и понятный переход в профессию. Детям и подросткам важны интерес, практика и ранний вход в техническое мышление без перегруза.",
        "cards": [
            {
                "title": "Для взрослых",
                "text": "Смена профессии, быстрый маршрут в сильный стек, практика, портфолио и собеседования без лишней теории.",
                "facts": ["смена профессии", "full-stack / backend", "карьерный переход"],
                "group": "development",
                "search": "смена профессии fullstack backend",
                "cta": "Показать взрослые маршруты",
            },
            {
                "title": "Для детей и подростков",
                "text": "Аккуратный старт через Python, проекты, ботов и понятные цифровые сценарии, где видно результат, а не только уроки.",
                "facts": ["Python Start", "проекты руками", "мягкий темп"],
                "group": "start",
                "search": "детям подросткам python",
                "cta": "Показать детские форматы",
            },
        ],
    },
    "teachers_showcase": {
        "eyebrow": "Преподаватели",
        "title": "Преподаватели, у которых вы реально учитесь",
        "description": (
            "Каждый наставник ведёт практику, разбирает решения и помогает держать темп до понятного результата."
        ),
        "note": "",
        "cards": [
            {
                "variant": "portrait",
                "panel_label": "",
                "panel_name": "",
                "name": "Преподаватель Full-stack",
                "role": "Full-stack, backend, карьерный переход",
                "lead": "Помогает собрать рабочий стек, не теряя связь между frontend, backend и реальными задачами.",
                "bio": (
                    "Разбирает архитектуру, код-ревью, API и подготовку к собеседованиям через проекты, которые можно показать в портфолио."
                ),
                "expertise": ["Full-stack", "API", "интервью", "портфолио"],
                "facts": ["сильный стек", "карьерный трек", "взрослая аудитория"],
                "initials": "FS",
                "photo": "",
                "alt": "Фото преподавателя Full-stack",
                "accent": "#6d52ff",
                "accent_soft": "rgba(109, 82, 255, 0.18)",
            },
            {
                "variant": "portrait",
                "panel_label": "",
                "panel_name": "",
                "name": "Преподаватель Backend",
                "role": "Python, FastAPI, базы данных",
                "lead": "Ведёт от базовой логики до сервисов, которые выглядят как настоящая backend-практика.",
                "bio": (
                    "Фокусируется на структуре кода, API, SQL, деплое и объяснении решений так, как это потребуется на рабочих задачах."
                ),
                "expertise": ["Python", "FastAPI", "SQL", "Docker"],
                "facts": ["код-ревью", "практика", "архитектура"],
                "initials": "BE",
                "photo": "",
                "alt": "Фото преподавателя Backend",
                "accent": "#4d7dff",
                "accent_soft": "rgba(77, 125, 255, 0.18)",
            },
            {
                "variant": "portrait",
                "panel_label": "",
                "panel_name": "",
                "name": "Преподаватель Data / QA",
                "role": "Аналитика, тестирование, автоматизация",
                "lead": "Помогает увидеть систему: данные, проверки, сценарии и понятную логику результата.",
                "bio": (
                    "Даёт практику по SQL, тест-дизайну, отчётам и автоматизации, чтобы ученик понимал не только инструмент, но и задачу."
                ),
                "expertise": ["SQL", "QA", "аналитика", "automation"],
                "facts": ["разбор задач", "мини-группа", "портфолио"],
                "initials": "DQ",
                "photo": "",
                "alt": "Фото преподавателя Data / QA",
                "accent": "#26b89a",
                "accent_soft": "rgba(38, 184, 154, 0.18)",
            },
        ],
    },
    "quiz": {
        "title": "Какое направление тебе подходит?",
        "description": "4 коротких ответа и система сама покажет, где ты быстрее увидишь результат.",
        "results": {
            "python_start": {
                "title": "Тебе подходит Python для новичков",
                "text": "Лучший вход, если нужен мягкий старт, база и первые проекты без перегруза.",
                "href": "/courses/python-beginners",
                "button": "Открыть стартовый курс",
            },
            "fullstack": {
                "title": "Тебе подходит Full-stack",
                "text": "Маршрут для тех, кто хочет сильную профессию, широкий стек и карьерный переход.",
                "href": "/courses/fullstack",
                "button": "Открыть Full-stack",
            },
            "data_science": {
                "title": "Тебе подходит Data Science / ML",
                "text": "Если тебя тянет в данные, модели и более высокий потолок зарплаты.",
                "href": "/courses/data-science",
                "button": "Открыть Data Science",
            },
            "business": {
                "title": "Тебе подходит Automation / Business",
                "text": "Самый короткий маршрут к прикладной пользе, ботам, интеграциям и первым деньгам.",
                "href": "/courses/business",
                "button": "Открыть Automation",
            },
        },
    },
}


HOME_COMPARE_BLUEPRINT: List[Dict[str, Any]] = [
    {
        "key": "python_start",
        "label": "Python Start",
        "badge": "Самый мягкий вход",
        "audience": "Для первого входа в IT и раннего старта для подростков.",
        "outcome": "База, маленькие проекты и уверенный следующий шаг.",
        "href": "/courses/python-beginners",
        "group": "start",
        "search": "с нуля новичкам python детям",
        "points": [
            "Понятная база без перегруза терминологией.",
            "Первые боты, мини-сервисы и прикладные задачи.",
            "Хороший маршрут, если важна мягкая скорость.",
        ],
    },
    {
        "key": "fullstack",
        "label": "Full-stack",
        "badge": "Для смены профессии",
        "audience": "Для взрослых, которым нужен системный карьерный переход.",
        "outcome": "Широкий стек, сильное портфолио и выход на собеседования.",
        "href": "/courses/fullstack",
        "group": "development",
        "search": "смена профессии fullstack backend",
        "points": [
            "Один из самых понятных маршрутов к junior-роли.",
            "Сильная связка фронта, бэка и инженерной логики.",
            "Подходит тем, кто хочет менять работу, а не просто учиться.",
        ],
    },
    {
        "key": "data_science",
        "label": "Data Science / ML",
        "badge": "Высокий потолок",
        "audience": "Для тех, кого тянет в данные, модели и техничную глубину.",
        "outcome": "Портфолио под стажировку, junior ML и прикладной DS.",
        "href": "/courses/data-science",
        "group": "analytics",
        "search": "data science ml данные высокий потолок",
        "points": [
            "Нужен интерес к данным, логике и экспериментам.",
            "Более длинный, но и более высокий потолок дохода.",
            "Маршрут для тех, кому важна техничная глубина.",
        ],
    },
    {
        "key": "business",
        "label": "Automation / Business",
        "badge": "Самый прикладной путь",
        "audience": "Для тех, кому важны быстрые задачи, польза и первые деньги.",
        "outcome": "Боты, интеграции и реальные automation-кейсы для бизнеса.",
        "href": "/courses/business",
        "group": "business",
        "search": "automation telegram боты бизнес быстрый старт",
        "points": [
            "Короткий путь к прикладной пользе и фриланс-задачам.",
            "Больше реальных сценариев, меньше абстрактной теории.",
            "Подходит тем, кто хочет увидеть отдачу быстрее.",
        ],
    },
]


def _deep_merge(base: Any, override: Any) -> Any:
    if not isinstance(base, dict) or not isinstance(override, dict):
        return deepcopy(override)

    payload = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(payload.get(key), dict):
            payload[key] = _deep_merge(payload[key], value)
        else:
            payload[key] = deepcopy(value)
    return payload


def _load_marketing_overrides() -> Dict[str, Any]:
    try:
        raw = MARKETING_CONFIG_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    return payload if isinstance(payload, dict) else {}


def _extract_numeric_value(raw: Any) -> int:
    if raw is None:
        return 0
    match = re.search(r"\d[\d\s]*", str(raw))
    if not match:
        return 0
    return int(match.group(0).replace(" ", ""))


def _extract_numeric_values(raw: Any) -> List[int]:
    if raw is None:
        return []
    return [int(item.replace(" ", "")) for item in re.findall(r"\d[\d\s]*", str(raw))]


def _format_number(value: int) -> str:
    return f"{max(0, int(value)):,}".replace(",", " ")


def _build_home_intent_nav() -> List[Dict[str, Any]]:
    return [
        {
            "label": "С нуля",
            "hint": "Мягкий вход и база",
            "group": "start",
            "search": "с нуля новичкам python",
        },
        {
            "label": "Смена профессии",
            "hint": "Взрослый карьерный переход",
            "group": "development",
            "search": "смена профессии fullstack backend",
        },
        {
            "label": "Для детей",
            "hint": "Аккуратный ранний старт",
            "group": "start",
            "search": "детям подросткам python",
        },
        {
            "label": "Быстрый старт",
            "hint": "Больше пользы и первых задач",
            "group": "business",
            "search": "automation telegram боты",
        },
        {
            "label": "Сравнить треки",
            "hint": "Увидеть разницу за 1 экран",
            "href": "#compare-tracks",
        },
    ]


def _build_home_compare_tracks() -> List[Dict[str, Any]]:
    tracks: List[Dict[str, Any]] = []
    for item in HOME_COMPARE_BLUEPRINT:
        marketing = get_track_marketing(item["key"])
        tracks.append(
            {
                **item,
                "salary_range": marketing["salary"]["range"],
                "time_to_offer": marketing["roi"]["time_to_offer"],
                "course_cost": _format_number(marketing["roi"]["course_cost"]),
                "summary": marketing["summary"],
                "slogan": marketing["slogan"],
            }
        )
    return tracks


def _build_course_decision_support(course: Dict[str, Any], marketing: Dict[str, Any]) -> Dict[str, Any]:
    category = marketing.get("category", "general")
    price_options = course.get("prices", [])
    featured_price = next(
        (_extract_numeric_value(item.get("new")) for item in price_options if item.get("featured")),
        0,
    )
    fallback_price = next((_extract_numeric_value(item.get("new")) for item in price_options), 0)
    price_from = featured_price or fallback_price
    package_options = [item for item in course.get("duration_options", []) if item != "Разово"]
    package_text = ", ".join(package_options[:4]) if package_options else "индивидуальному графику"
    pace_value = ""
    hero_bullets = course.get("hero_bullets", [])
    if len(hero_bullets) > 1:
        pace_value = hero_bullets[1].get("value", "")

    not_for_map = {
        "start": [
            {
                "title": "Если нужен мгновенный результат без базы",
                "text": "Стартовый трек всё равно требует регулярной практики и терпения к основам.",
            },
            {
                "title": "Если не хочется делать задания руками",
                "text": "Здесь быстро начинается практика, а не пассивный просмотр уроков.",
            },
            {
                "title": "Если нет времени на устойчивый ритм",
                "text": "Даже мягкий формат требует хотя бы нескольких часов в неделю на закрепление.",
            },
        ],
        "development": [
            {
                "title": "Если нужен только обзор без глубины",
                "text": "Трек рассчитан на код, архитектуру и проектную дисциплину, а не на поверхностное знакомство.",
            },
            {
                "title": "Если не хочется разбираться в ошибках и рефакторинге",
                "text": "Рост в разработке идёт через разбор кода, а не только через просмотр готовых решений.",
            },
            {
                "title": "Если нет ресурса на практику каждую неделю",
                "text": "Маршрут работает только при регулярном ритме, особенно если цель — смена профессии.",
            },
        ],
        "analytics": [
            {
                "title": "Если не нравятся цифры и логика",
                "text": "Здесь много внимательности к данным, гипотезам и структурированным выводам.",
            },
            {
                "title": "Если нужен только визуальный результат без анализа",
                "text": "Основная ценность трека — в мышлении, моделях и интерпретации, а не в внешнем эффекте.",
            },
            {
                "title": "Если не готовы к более длинному горизонту роста",
                "text": "Сильные data-направления окупаются хорошо, но требуют больше концентрации и времени.",
            },
        ],
        "quality": [
            {
                "title": "Если не любите детали",
                "text": "QA требует внимательности, системности и привычки замечать слабые места продукта.",
            },
            {
                "title": "Если нужен только креатив без структуры",
                "text": "Здесь важны сценарии, проверки и дисциплина мышления.",
            },
            {
                "title": "Если не готовы работать с документацией и кейсами",
                "text": "Проверка качества опирается на понятные артефакты, а не только на интуицию.",
            },
        ],
        "infrastructure": [
            {
                "title": "Если не нравится системность",
                "text": "Инфраструктурные роли требуют аккуратности, стабильности и внимания к окружению.",
            },
            {
                "title": "Если хочется только быстрых визуальных результатов",
                "text": "Здесь ценность чаще скрыта внутри процессов, надёжности и автоматизации.",
            },
            {
                "title": "Если не готовы разбираться с настройками и средой",
                "text": "Трек опирается на реальные серверные и platform-задачи, а не на облегчённые демо.",
            },
        ],
        "business": [
            {
                "title": "Если хочется только теории про бизнес без реализации",
                "text": "Основной результат здесь — работающие боты, интеграции и automation-сценарии.",
            },
            {
                "title": "Если неинтересны прикладные задачи клиентов",
                "text": "Этот маршрут сильнее всего раскрывается именно в реальных кейсах и пользе.",
            },
            {
                "title": "Если не готовы быстро пробовать и тестировать гипотезы",
                "text": "Здесь важна инициативность и желание быстрее увидеть рабочий результат.",
            },
        ],
    }

    return {
        "section_nav": [
            {"label": "Рынок", "href": "#market"},
            {"label": "Стоимость", "href": "#pricing"},
            {"label": "Программа", "href": "#program"},
            {"label": "Траектория", "href": "#journey"},
            {"label": "FAQ", "href": "#faq"},
            {"label": "Запись", "href": "#enroll"},
        ],
        "finance_facts": [
            {
                "title": "Формат оплаты",
                "text": f"Можно идти разово или пакетами на {package_text}. Чем стабильнее горизонт, тем выгоднее ставка.",
            },
            {
                "title": "Что входит в цену",
                "text": "Мини-группа, практика, разбор кода, карьерный слой, помощь с GitHub, резюме и техническими интервью.",
            },
            {
                "title": "Как быстро стартуем",
                "text": "После оплаты фиксируем место, смотрим уровень, согласуем ритм и выдаём стартовый маршрут уже на первую неделю.",
            },
            {
                "title": "Финансовый ориентир",
                "text": (
                    f"Текущий вход от {_format_number(price_from)} ₽ и горизонт до первой оплачиваемой роли около "
                    f"{marketing['roi']['time_to_offer']} мес."
                    if price_from
                    else f"Первая оплачиваемая роль обычно появляется через {marketing['roi']['time_to_offer']} мес."
                ),
            },
        ],
        "next_steps": [
            {
                "title": "Оставляете заявку",
                "text": "Фиксируем интерес к треку, отвечаем на вопросы и бронируем место по текущему офферу.",
            },
            {
                "title": "Подтверждаем оплату",
                "text": "Высылаем документы, закрепляем формат и проговариваем удобный ритм занятий.",
            },
            {
                "title": "Собираем стартовый план",
                "text": "Смотрим текущий уровень, график и цель, чтобы не вести вас по чужому шаблону.",
            },
            {
                "title": "Входим в рабочий контур",
                "text": (
                    f"Подключаем к практике, задаём первый вектор и выходим в устойчивый темп {pace_value}."
                    if pace_value
                    else "Подключаем к практике, задаём первый вектор и выходим в устойчивый темп."
                ),
            },
        ],
        "not_for": not_for_map.get(
            category,
            [
                {
                    "title": "Если нужен только пассивный просмотр",
                    "text": "Формат построен на практике, а не на накоплении непрожитой теории.",
                },
                {
                    "title": "Если не готовы выделять время каждую неделю",
                    "text": "Даже гибкий маршрут требует устойчивого ритма, иначе прогресс размывается.",
                },
                {
                    "title": "Если важна только бумага, а не навык",
                    "text": "Здесь основной результат — проекты, портфолио и рыночная уверенность.",
                },
            ],
        ),
    }


ROI_PAGE_MARKETING: Dict[str, Any] = {
    "page_intro": {
        "eyebrow": "Окупаемость курса",
        "title": "Поймите не только чему учиться, но и когда обучение начинает приносить деньги",
        "text": (
            "Это отдельная страница для честного финансового сценария: какой трек выбрать, через сколько месяцев "
            "обычно появляется первая оплачиваемая роль и в какой момент курс перестаёт быть расходом."
        ),
        "highlights": [
            {"title": "Сценарии темпа", "text": "Спокойный, рабочий и интенсивный режим входа без магических обещаний."},
            {"title": "Стартовый доход", "text": "Ориентиры по реальным junior-ролям, а не по абстрактному потолку рынка."},
            {"title": "Доход за первый год", "text": "Показываем не только payback, но и потенциальный денежный горизонт после старта."},
        ],
    },
    "explanation_steps": [
        {
            "title": "1. Выбираете трек",
            "text": "Сравниваете направления по стартовой вилке, скорости выхода на первую роль и входному чеку.",
        },
        {
            "title": "2. Ставите цель по доходу",
            "text": "Можно взять стартовую цифру, комфортный сценарий или свою конкретную зарплатную цель.",
        },
        {
            "title": "3. Видите горизонт окупаемости",
            "text": "Калькулятор отдельно считает время до первой оплачиваемой задачи и время до возврата вложений.",
        },
    ],
    "decision_notes": [
        {
            "title": "Окупаемость не равна зарплате",
            "text": "Мы закладываем только часть первого дохода на возврат курса, чтобы расчёт был ближе к реальной жизни.",
        },
        {
            "title": "Первый доход важнее идеального оффера",
            "text": "Для многих треков быстрее приходят первые проекты, фриланс или стажировки, а уже потом сильный найм.",
        },
        {
            "title": "Темп критичен",
            "text": "Один и тот же курс даёт разный горизонт окупаемости в зависимости от плотности практики и регулярности.",
        },
    ],
    "cta": {
        "title": "Нужен расчёт под вашу ситуацию?",
        "text": "Подберём трек, темп и стартовый маршрут так, чтобы окупаемость считалась не в вакууме, а под ваш график и цель.",
        "button": "Получить персональный расчёт",
    },
}


_MARKETING_OVERRIDES = _load_marketing_overrides()
MARKETING_RUNTIME = _deep_merge(DEFAULT_MARKETING_RUNTIME, _MARKETING_OVERRIDES.get("runtime", {}))
TRACK_MARKETING = _deep_merge(TRACK_MARKETING, _MARKETING_OVERRIDES.get("tracks", {}))
HOME_MARKETING = _deep_merge(HOME_MARKETING, _MARKETING_OVERRIDES.get("home", {}))
ROI_PAGE_MARKETING = _deep_merge(ROI_PAGE_MARKETING, _MARKETING_OVERRIDES.get("roi_page", {}))

TEACHER_SHOWCASE_ACCENTS = {
    "violet": ("#6d52ff", "rgba(109, 82, 255, 0.18)"),
    "mint": ("#28b894", "rgba(40, 184, 148, 0.18)"),
    "sky": ("#2f80ed", "rgba(47, 128, 237, 0.18)"),
    "sunset": ("#ff8a5b", "rgba(255, 138, 91, 0.18)"),
}


def build_marketing_runtime() -> Dict[str, Any]:
    return deepcopy(MARKETING_RUNTIME)


def _build_teacher_showcase_cards() -> List[Dict[str, Any]]:
    try:
        from routes.account_content import load_teachers

        teachers = [item for item in load_teachers() if item.get("status") == "active"]
    except Exception:
        return []
    if not teachers:
        return []

    cards = []
    compact_variant = len(teachers) > 1
    for teacher in teachers:
        disciplines = teacher.get("disciplines") if isinstance(teacher.get("disciplines"), list) else []
        expertise = teacher.get("expertise") if isinstance(teacher.get("expertise"), list) else []
        course_names = disciplines or ([teacher["speciality"]] if teacher.get("speciality") else [])
        primary_course = course_names[0] if course_names else "Индивидуальный маршрут"
        accent, accent_soft = TEACHER_SHOWCASE_ACCENTS.get(
            str(teacher.get("accent") or "").strip(),
            TEACHER_SHOWCASE_ACCENTS["violet"],
        )
        cards.append(
            {
                "variant": "avatar" if compact_variant else "portrait",
                "panel_label": "Преподаватель",
                "panel_name": primary_course,
                "name": teacher["name"],
                "role": teacher["role"],
                "lead": (
                    f"Ведёт {', '.join(course_names[:2])}."
                    if course_names
                    else "Помогает выстроить понятный темп и учебный маршрут."
                ),
                "bio": teacher.get("bio") or "Работает с практикой, разбором ошибок и понятным движением по программе.",
                "expertise": (expertise or course_names or [teacher["role"]])[:5],
                "facts": (course_names or expertise or [primary_course])[:4],
                "initials": teacher["initials"],
                "photo": str(teacher.get("photo") or "").strip(),
                "alt": f"Фото преподавателя {teacher['name']}",
                "accent": accent,
                "accent_soft": accent_soft,
                "href": f"/teachers/{teacher['id']}",
            }
        )
    return cards


def save_marketing_runtime_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    global _MARKETING_OVERRIDES, MARKETING_RUNTIME

    overrides = _load_marketing_overrides()
    runtime_payload = _deep_merge(overrides.get("runtime", {}), updates)
    overrides["runtime"] = runtime_payload
    MARKETING_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    MARKETING_CONFIG_FILE.write_text(json.dumps(overrides, ensure_ascii=False, indent=2), encoding="utf-8")

    _MARKETING_OVERRIDES = overrides
    MARKETING_RUNTIME = _deep_merge(DEFAULT_MARKETING_RUNTIME, runtime_payload)
    return build_marketing_runtime()


def build_homepage_marketing() -> Dict[str, Any]:
    payload = deepcopy(HOME_MARKETING)
    payload["promo"] = deepcopy(MARKETING_RUNTIME["promo"])
    _refresh_offer_deadline(payload["promo"])
    payload["video"] = deepcopy(MARKETING_RUNTIME["video"])
    payload["intent_nav"] = _build_home_intent_nav()
    payload["compare_tracks"] = _build_home_compare_tracks()
    teacher_cards = _build_teacher_showcase_cards()
    if teacher_cards:
        fallback_teacher_cards = deepcopy(HOME_MARKETING["teachers_showcase"]["cards"])
        if len(teacher_cards) < 3:
            teacher_cards.extend(fallback_teacher_cards[: 3 - len(teacher_cards)])
        payload["teachers_showcase"] = deepcopy(payload.get("teachers_showcase", {}))
        payload["teachers_showcase"]["description"] = (
            "Наставники ведут мини-группы, проверяют практику и помогают держать маршрут до результата."
        )
        payload["teachers_showcase"]["note"] = ""
        payload["teachers_showcase"]["cards"] = teacher_cards
    roi_visuals = {
        "python_start": {
            "accent": "#ff9a62",
            "accent_soft": "rgba(255, 154, 98, 0.18)",
            "href": "/courses/python-beginners",
        },
        "fullstack": {
            "accent": "#6a8dff",
            "accent_soft": "rgba(106, 141, 255, 0.18)",
            "href": "/courses/fullstack",
        },
        "data_science": {
            "accent": "#26b89a",
            "accent_soft": "rgba(38, 184, 154, 0.18)",
            "href": "/courses/data-science",
        },
        "business": {
            "accent": "#ff6d8f",
            "accent_soft": "rgba(255, 109, 143, 0.18)",
            "href": "/courses/business",
        },
    }
    payload["roi_tracks"] = [
        {
            "key": key,
            "label": label,
            "salary_range": meta["salary"]["range"],
            "salary_note": meta["salary"]["label"],
            "salary_min": (_extract_numeric_values(meta["salary"]["range"]) + [meta["roi"]["entry_salary"]])[0],
            "salary_max": (
                _extract_numeric_values(meta["salary"]["range"]) + [meta["roi"]["entry_salary"], meta["roi"]["entry_salary"]]
            )[-1],
            "course_cost": meta["roi"]["course_cost"],
            "time_to_offer": meta["roi"]["time_to_offer"],
            "entry_salary": meta["roi"]["entry_salary"],
            "summary": meta["summary"],
            "slogan": meta["slogan"],
            "alumni_result": meta["alumni"]["result"],
            "alumni_timeline": meta["alumni"]["timeline"],
            "accent": roi_visuals[key]["accent"],
            "accent_soft": roi_visuals[key]["accent_soft"],
            "href": roi_visuals[key]["href"],
        }
        for key, label, meta in (
            ("python_start", "Python Start", TRACK_MARKETING["python_start"]),
            ("fullstack", "Full-stack", TRACK_MARKETING["fullstack"]),
            ("data_science", "Data Science / ML", TRACK_MARKETING["data_science"]),
            ("business", "Automation / Business", TRACK_MARKETING["business"]),
        )
    ]
    payload["roi_scenarios"] = [
        {
            "key": "steady",
            "label": "Спокойный темп",
            "description": "Комфортный ритм с мягким входом и запасом по срокам.",
            "offer_shift": 1,
            "salary_factor": 0.9,
            "payback_share": 0.25,
        },
        {
            "key": "balanced",
            "label": "Рабочий ритм",
            "description": "Базовый сценарий: стабильная практика и нормальный темп.",
            "offer_shift": 0,
            "salary_factor": 0.97,
            "payback_share": 0.35,
        },
        {
            "key": "intense",
            "label": "Интенсив",
            "description": "Плотная практика и более быстрый выход на коммерцию.",
            "offer_shift": -1,
            "salary_factor": 1.05,
            "payback_share": 0.45,
        },
    ]
    return payload


def build_roi_page_marketing() -> Dict[str, Any]:
    payload = build_homepage_marketing()
    payload.update(deepcopy(ROI_PAGE_MARKETING))
    return payload


def get_track_marketing(course_key: str) -> Dict[str, Any]:
    default = {
        "category": "general",
        "slogan": "Чёткий маршрут в профессию без лишней воды.",
        "summary": "Коротко, ясно и по делу.",
        "search_tags": [],
        "salary": {"range": "80 000 - 140 000 ₽", "label": "Ориентир по рынку"},
        "roi": {"course_cost": 68000, "time_to_offer": 5, "entry_salary": 95000},
        "offer": deepcopy(MARKETING_RUNTIME["promo"]),
        "alumni": {
            "name": "Студент Peaky Minds",
            "result": "портфолио и реальные кейсы",
            "timeline": "движение к первой оплачиваемой задаче",
            "href": "/blog",
        },
    }
    if course_key not in TRACK_MARKETING:
        _refresh_offer_deadline(default["offer"])
        return default
    payload = deepcopy(default)
    payload.update(deepcopy(TRACK_MARKETING[course_key]))
    _refresh_offer_deadline(payload["offer"])
    return payload


def decorate_course(course_key: str, course: Dict[str, Any]) -> Dict[str, Any]:
    payload = deepcopy(course)
    payload["marketing"] = get_track_marketing(course_key)
    payload["decision_support"] = _build_course_decision_support(payload, payload["marketing"])
    return payload


def decorate_catalog_page(courses_page: Dict[str, Any]) -> Dict[str, Any]:
    payload = deepcopy(courses_page)
    payload["quick_filters"] = deepcopy(HOME_MARKETING["quick_filters"])
    payload["courses"] = [
        {
            **item,
            "marketing": get_track_marketing(item["key"]),
        }
        for item in payload.get("courses", [])
    ]
    return payload
