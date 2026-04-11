from copy import deepcopy
from typing import Any, Dict


MARKETING_RUNTIME: Dict[str, Any] = {
    "promo": {
        "label": "Спецпредложение апреля",
        "discount": "-20%",
        "deadline_iso": "2026-05-01T23:59:59+03:00",
        "deadline_label": "до 1 мая",
        "seats_left": 3,
        "summary": "Фиксируем место в мини-группе и текущую цену до конца акции.",
    },
    "sticky_bar": {
        "title": "Сомневаешься? Задай вопрос в Telegram за 30 секунд.",
        "button": "Открыть Telegram",
    },
    "video": {
        "title": "Промо-ролик Peaky Minds",
        "eyebrow": "Скоро на главном экране",
        "description": "Секция и видеоплеер готовы к интеграции мастер-ролика. После загрузки MP4 или YouTube-ссылки блок подхватит видео без перепаковки шаблона.",
        "duration": "45-60 секунд",
        "video_url": "",
        "poster": "/assets/img/logo.png",
    },
}


TRACK_MARKETING: Dict[str, Dict[str, Any]] = {
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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
            "deadline_label": "до 1 мая",
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


def build_marketing_runtime() -> Dict[str, Any]:
    return deepcopy(MARKETING_RUNTIME)


def build_homepage_marketing() -> Dict[str, Any]:
    payload = deepcopy(HOME_MARKETING)
    payload["promo"] = deepcopy(MARKETING_RUNTIME["promo"])
    payload["video"] = deepcopy(MARKETING_RUNTIME["video"])
    payload["roi_tracks"] = [
        {
            "key": key,
            "label": label,
            "salary_range": meta["salary"]["range"],
            "course_cost": meta["roi"]["course_cost"],
            "time_to_offer": meta["roi"]["time_to_offer"],
            "entry_salary": meta["roi"]["entry_salary"],
        }
        for key, label, meta in (
            ("python_start", "Python Start", TRACK_MARKETING["python_start"]),
            ("fullstack", "Full-stack", TRACK_MARKETING["fullstack"]),
            ("data_science", "Data Science / ML", TRACK_MARKETING["data_science"]),
            ("business", "Automation / Business", TRACK_MARKETING["business"]),
        )
    ]
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
        return default
    payload = deepcopy(default)
    payload.update(deepcopy(TRACK_MARKETING[course_key]))
    return payload


def decorate_course(course_key: str, course: Dict[str, Any]) -> Dict[str, Any]:
    payload = deepcopy(course)
    payload["marketing"] = get_track_marketing(course_key)
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
