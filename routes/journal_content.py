from copy import deepcopy
from typing import Any, Dict, List, Optional

from core import JOURNAL_POSTS_FILE, load_json, save_json


JOURNAL_CATEGORY_META: Dict[str, Dict[str, str]] = {
    "programming": {"label": "Программирование"},
    "analytics": {"label": "Аналитика"},
    "automation": {"label": "Автоматизация"},
    "design": {"label": "Дизайн"},
    "career": {"label": "Карьера"},
    "stories": {"label": "Истории студентов"},
}

JOURNAL_CATEGORY_KEYS = {"all", *JOURNAL_CATEGORY_META.keys()}

DEFAULT_JOURNAL_POSTS: List[Dict[str, Any]] = [
    {
        "slug": "telegram-bot-enrollment-flow",
        "category": "programming",
        "title": "Как собрать Telegram-бота для записи учеников и не утонуть в хаосе",
        "excerpt": (
            "Разбираем архитектуру простого бота для школы: FastAPI, webhook, статусы заявок и "
            "точка входа для менеджера без ручной рутины."
        ),
        "date": "26 марта 2026",
        "reading_time": "8 мин",
        "likes": 96,
        "comments": 12,
        "views": "12 480",
        "author": "Михаил Павлов",
        "author_role": "Backend mentor",
        "art": {
            "kicker": "FastAPI",
            "headline": "Bot Flow",
            "note": "Webhook · заявки · CRM handoff",
            "primary": "#6d52ff",
            "secondary": "#32d58a",
            "tertiary": "#9db0ff",
        },
    },
    {
        "slug": "design-studio-one-case",
        "category": "design",
        "title": "Как работает дизайн-студия на примере одного кейса",
        "excerpt": (
            "Показываем путь от хаотичного запроса клиента до понятного интерфейса: исследование, "
            "каркас, визуальная система и handoff в разработку."
        ),
        "date": "22 марта 2026",
        "reading_time": "7 мин",
        "likes": 58,
        "comments": 6,
        "views": "8 920",
        "author": "Владимир Кондратьев",
        "author_role": "Product designer",
        "art": {
            "kicker": "Design Ops",
            "headline": "Case Build",
            "note": "Исследование · wireframe · UI kit",
            "primary": "#7a63ff",
            "secondary": "#53c6ff",
            "tertiary": "#b7b1ff",
        },
    },
    {
        "slug": "junior-analyst-actually-does",
        "category": "analytics",
        "title": "Что на самом деле делает junior data analyst в первые три месяца",
        "excerpt": (
            "Без мифов о магии данных. Какие SQL-задачи дают новичку, где ломается логика и "
            "почему бизнес-контекст важнее количества инструментов в резюме."
        ),
        "date": "19 марта 2026",
        "reading_time": "6 мин",
        "likes": 74,
        "comments": 9,
        "views": "10 144",
        "author": "Анна Новикова",
        "author_role": "Data analyst mentor",
        "art": {
            "kicker": "SQL + BI",
            "headline": "Data Map",
            "note": "Запросы · витрины · гипотезы",
            "primary": "#4d7dff",
            "secondary": "#32d58a",
            "tertiary": "#8ec5ff",
        },
    },
    {
        "slug": "automation-scenarios-for-small-business",
        "category": "automation",
        "title": "7 сценариев автоматизации малого бизнеса без отдельной команды разработки",
        "excerpt": (
            "От заявок и напоминаний до простых внутренних дашбордов. Смотрим, где код реально "
            "экономит часы и что можно внедрить уже на старте."
        ),
        "date": "14 марта 2026",
        "reading_time": "9 мин",
        "likes": 83,
        "comments": 11,
        "views": "11 302",
        "author": "Илья Сергеев",
        "author_role": "Automation engineer",
        "art": {
            "kicker": "Automation",
            "headline": "Ops Stack",
            "note": "боты · CRM · уведомления",
            "primary": "#32d58a",
            "secondary": "#6d52ff",
            "tertiary": "#9effd0",
        },
    },
    {
        "slug": "pet-project-that-sells-you",
        "category": "career",
        "title": "Как оформить pet-project так, чтобы он продавал ваш опыт, а не просто лежал на GitHub",
        "excerpt": (
            "Что нужно показать работодателю кроме кода: сценарий использования, ограничения, "
            "trade-offs и следующую версию решения."
        ),
        "date": "10 марта 2026",
        "reading_time": "8 мин",
        "likes": 91,
        "comments": 17,
        "views": "13 906",
        "author": "Peaky Minds Team",
        "author_role": "Career guidance",
        "art": {
            "kicker": "Portfolio",
            "headline": "Proof of Work",
            "note": "README · demo · решения",
            "primary": "#ff7a59",
            "secondary": "#6d52ff",
            "tertiary": "#ffc1ad",
        },
    },
    {
        "slug": "git-without-panic",
        "category": "programming",
        "title": "Git без паники: что должен уметь junior после первого месяца практики",
        "excerpt": (
            "Коммиты, ветки, rebase и pull request. Не как набор слов, а как рабочий процесс, "
            "который не ломает команду и не тормозит релиз."
        ),
        "date": "06 марта 2026",
        "reading_time": "5 мин",
        "likes": 66,
        "comments": 8,
        "views": "9 410",
        "author": "Михаил Павлов",
        "author_role": "Backend mentor",
        "art": {
            "kicker": "Git",
            "headline": "Branch Logic",
            "note": "commit · review · merge",
            "primary": "#1f2430",
            "secondary": "#6d52ff",
            "tertiary": "#6bffcb",
        },
    },
    {
        "slug": "from-office-to-it-story",
        "category": "stories",
        "title": "Из офиса в IT за 8 месяцев: как студент сменил рутину на backend-практику",
        "excerpt": (
            "Не история про волшебный скачок. История про дисциплину, реальные задачи, первые "
            "ошибки и спокойный выход на собеседования."
        ),
        "date": "02 марта 2026",
        "reading_time": "7 мин",
        "likes": 103,
        "comments": 14,
        "views": "14 620",
        "author": "Илья Морозов",
        "author_role": "Студент backend-track",
        "art": {
            "kicker": "Student Story",
            "headline": "Backend Switch",
            "note": "8 месяцев · практика · офферы",
            "primary": "#6d52ff",
            "secondary": "#ff8e5c",
            "tertiary": "#c8c0ff",
        },
    },
    {
        "slug": "sql-interview-task-breakdown",
        "category": "analytics",
        "title": "Разбор SQL-задачи с техсобеседования на аналитика: где чаще всего ломаются ответы",
        "excerpt": (
            "Смотрим одну реальную задачу, разбираем типичные ошибки в JOIN и объясняем, как "
            "озвучивать ход мысли так, чтобы интервьюер видел зрелое мышление."
        ),
        "date": "28 февраля 2026",
        "reading_time": "10 мин",
        "likes": 88,
        "comments": 13,
        "views": "12 044",
        "author": "Анна Новикова",
        "author_role": "Data analyst mentor",
        "art": {
            "kicker": "Interview",
            "headline": "SQL Drill",
            "note": "JOIN · window · reasoning",
            "primary": "#4d7dff",
            "secondary": "#6d52ff",
            "tertiary": "#96d1ff",
        },
    },
    {
        "slug": "first-freelance-money",
        "category": "stories",
        "title": "Первые деньги на фрилансе после второго модуля: что именно сработало",
        "excerpt": (
            "Разбираем, как студент оформил навык, нашёл первую задачу, договорился о результате "
            "и не провалил дедлайн на старте."
        ),
        "date": "23 февраля 2026",
        "reading_time": "6 мин",
        "likes": 79,
        "comments": 10,
        "views": "11 114",
        "author": "Даниил Петров",
        "author_role": "Студент automation-track",
        "art": {
            "kicker": "Freelance",
            "headline": "First Invoice",
            "note": "бриф · дедлайн · оплата",
            "primary": "#32d58a",
            "secondary": "#4d7dff",
            "tertiary": "#b1ffe1",
        },
    },
    {
        "slug": "telegram-task-tracker",
        "category": "automation",
        "title": "Как настроить внутренний трекер задач в Telegram без тяжёлой enterprise-системы",
        "excerpt": (
            "Когда команде нужен понятный процесс, а не ещё один дорогой инструмент. Схема для "
            "небольшой школы, агентства или сервиса."
        ),
        "date": "18 февраля 2026",
        "reading_time": "7 мин",
        "likes": 63,
        "comments": 5,
        "views": "8 604",
        "author": "Илья Сергеев",
        "author_role": "Automation engineer",
        "art": {
            "kicker": "Workflow",
            "headline": "Task Loop",
            "note": "статусы · reminders · owner",
            "primary": "#25c6ff",
            "secondary": "#32d58a",
            "tertiary": "#a7efff",
        },
    },
    {
        "slug": "interview-without-cramming",
        "category": "career",
        "title": "Как готовиться к техсобеседованию без зубрёжки и бессмысленных списков вопросов",
        "excerpt": (
            "Важно не просто что-то выучить, а уметь спокойно объяснить, почему вы написали "
            "именно так и как бы улучшали решение в реальном проекте."
        ),
        "date": "12 февраля 2026",
        "reading_time": "9 мин",
        "likes": 112,
        "comments": 19,
        "views": "15 708",
        "author": "Peaky Minds Team",
        "author_role": "Career guidance",
        "art": {
            "kicker": "Career",
            "headline": "Reasoning",
            "note": "trade-offs · код · собеседование",
            "primary": "#ff8e5c",
            "secondary": "#ffd24d",
            "tertiary": "#ffd9c8",
        },
    },
    {
        "slug": "frontend-portfolio-package",
        "category": "design",
        "title": "Как собрать портфолио frontend-разработчика, если реальных проектов пока мало",
        "excerpt": (
            "Показываем связку: мини-кейсы, README, адаптив, объяснение решений и как это всё "
            "упаковать в убедимую историю роста."
        ),
        "date": "08 февраля 2026",
        "reading_time": "8 мин",
        "likes": 69,
        "comments": 7,
        "views": "9 982",
        "author": "Владимир Кондратьев",
        "author_role": "Product designer",
        "art": {
            "kicker": "Frontend",
            "headline": "Case Pack",
            "note": "UI · deploy · narrative",
            "primary": "#6d52ff",
            "secondary": "#25c6ff",
            "tertiary": "#c0d3ff",
        },
    },
]

POST_BODY_META: Dict[str, Dict[str, Any]] = {
    "programming": {
        "heading": "Что обычно недооценивают в инженерной практике",
        "lead": (
            "Новичок часто смотрит на тему как на набор команд, но реальная работа начинается "
            "там, где нужно объяснить ход мысли, ограничения и последствия выбранного решения."
        ),
        "points": [
            "Показывайте не только код, но и сценарий, который этот код обслуживает.",
            "Фиксируйте структуру проекта и договорённости по именованию до того, как станет больно поддерживать репозиторий.",
            "Любое решение полезно сопровождать коротким объяснением trade-offs: что вы упростили и почему.",
        ],
        "quote": "Хороший junior не угадывает идеальный ответ, а последовательно объясняет логику и способен защитить свои решения.",
    },
    "analytics": {
        "heading": "Почему аналитика ломается не в SQL, а в формулировке вопроса",
        "lead": (
            "Сильный аналитик сначала уточняет бизнес-контекст, а уже потом пишет запрос. "
            "Именно это отличает случайное решение от полезного для команды результата."
        ),
        "points": [
            "Перед запросом нужно проговорить, какая метрика считается успешной и какой период берётся в расчёт.",
            "Чистый SQL без внятного комментария не помогает, если следующий человек не понимает, какую гипотезу вы проверяли.",
            "На собеседовании ценят не скорость набора, а способность увидеть дыры в данных и честно их обозначить.",
        ],
        "quote": "Аналитика ценится там, где числа превращаются в управленческое решение, а не в красивую витрину без контекста.",
    },
    "automation": {
        "heading": "Где автоматизация реально окупается",
        "lead": (
            "Автоматизация полезна не там, где можно написать код ради самого кода, а там, "
            "где повторяемая операция мешает росту команды и крадёт внимание у людей."
        ),
        "points": [
            "Начинать стоит с ручных процессов, которые повторяются каждую неделю и дают одни и те же ошибки.",
            "Инструмент должен быть прозрачен: кто владелец, где статусы, как откатиться, если сценарий дал сбой.",
            "Лучше один понятный automation-flow, чем пять полуготовых скриптов без поддержки и документации.",
        ],
        "quote": "Автоматизация оправдана там, где она снимает операционную боль, а не просто выглядит технологично.",
    },
    "design": {
        "heading": "Почему визуал без системы быстро становится дорогой проблемой",
        "lead": (
            "Интерфейс выигрывает не за счёт случайной красоты, а за счёт понятной структуры, "
            "повторяемых решений и ясного handoff между дизайнером и разработчиком."
        ),
        "points": [
            "Сильный кейс показывает путь от проблемы пользователя к интерфейсному решению, а не только финальный экран.",
            "Компонентная система экономит недели правок, когда продукт начинает расти за пределы одного лендинга.",
            "Даже маленькое портфолио выглядит сильнее, если видно, как вы принимаете решения и что тестировали.",
        ],
        "quote": "Хороший интерфейс не объясняет себя дизайнеру, он объясняет себя пользователю и команде разработки.",
    },
    "career": {
        "heading": "Что действительно оценивают на старте карьеры",
        "lead": (
            "Работодателю редко нужен человек, который вызубрил идеальные формулировки. Нужен "
            "человек, который спокойно мыслит, умеет признавать ограничения и предлагает разумный следующий шаг."
        ),
        "points": [
            "Резюме и pet-project должны показывать мышление, а не только список технологий.",
            "На интервью полезнее проговаривать рассуждение, чем пытаться любой ценой выдать мгновенно правильный ответ.",
            "Портфолио работает лучше, когда из него видно, как вы принимали решения и что бы улучшили при следующей итерации.",
        ],
        "quote": "Карьерный рост начинается в тот момент, когда вы перестаёте демонстрировать стек и начинаете демонстрировать зрелость решений.",
    },
    "stories": {
        "heading": "Почему истории студентов важнее мотивационных лозунгов",
        "lead": (
            "Реальный переход в IT редко выглядит как красивая рекламная дуга. Обычно это "
            "серия маленьких шагов, ошибок, повторений и нескольких первых побед, которые дают уверенность идти дальше."
        ),
        "points": [
            "Полезна не только точка успеха, но и детали пути: сколько времени ушло, где был провал, как человек из него вышел.",
            "История становится убедительной, когда есть конкретные действия: проект, отклики, практика, первые деньги, собеседования.",
            "Такие кейсы помогают новым ученикам понять, что рост строится на ритме и системе, а не на одном удачном моменте.",
        ],
        "quote": "Сильная история ученика снимает иллюзию волшебного прорыва и показывает нормальную рабочую траекторию роста.",
    },
}


def _build_default_store() -> Dict[str, Any]:
    return {"version": 1, "items": deepcopy(DEFAULT_JOURNAL_POSTS)}


def load_journal_posts_payload() -> Dict[str, Any]:
    default_payload = _build_default_store()
    payload = load_json(JOURNAL_POSTS_FILE, default_payload)
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list) or not payload.get("items"):
        payload = default_payload
        save_json(JOURNAL_POSTS_FILE, payload)
    elif not JOURNAL_POSTS_FILE.exists():
        save_json(JOURNAL_POSTS_FILE, payload)
    return payload


def _post_initials(name: str) -> str:
    parts = [item for item in str(name).split() if item]
    if not parts:
        return "PM"
    return "".join(part[0] for part in parts[:2]).upper()


def _build_post_body(post: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = post.get("body")
    if isinstance(body, list) and body:
        return body

    meta = POST_BODY_META.get(post["category"], POST_BODY_META["career"])
    return [
        {
            "type": "paragraph",
            "text": (
                f"{post['excerpt']} Ниже не теория ради теории, а практический разбор того, как эта тема "
                "выглядит в нормальной рабочей среде и почему она важна для роста в IT."
            ),
        },
        {"type": "heading", "text": meta["heading"]},
        {"type": "paragraph", "text": meta["lead"]},
        {"type": "list", "items": meta["points"]},
        {"type": "quote", "text": meta["quote"]},
        {"type": "heading", "text": "Как использовать это в обучении"},
        {
            "type": "paragraph",
            "text": (
                "Сильнее всего эта тема закрепляется не чтением заметки, а маленьким проектом, обсуждением "
                "решения и рефлексией: почему вы сделали именно так, что упростили и как улучшили бы реализацию "
                "в следующей итерации."
            ),
        },
    ]


def _normalize_post(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    slug = str(item.get("slug") or "").strip()
    category = str(item.get("category") or "").strip()
    title = str(item.get("title") or "").strip()
    excerpt = str(item.get("excerpt") or "").strip()
    if not slug or category not in JOURNAL_CATEGORY_META or not title or not excerpt:
        return None

    author_name = str(item.get("author") or "Редакция Peaky Minds").strip()
    author_role = str(item.get("author_role") or "Peaky Minds").strip()
    art = item.get("art") if isinstance(item.get("art"), dict) else {}
    enriched = {
        **item,
        "slug": slug,
        "category": category,
        "title": title,
        "excerpt": excerpt,
        "category_label": JOURNAL_CATEGORY_META[category]["label"],
        "url": f"/posts/{slug}",
        "author": {
            "name": author_name,
            "role": author_role,
            "initials": _post_initials(author_name),
        },
        "body": _build_post_body(item),
        "tags": [
            value
            for value in dict.fromkeys(
                [
                    JOURNAL_CATEGORY_META[category]["label"],
                    str(art.get("kicker") or "").strip(),
                    str(item.get("reading_time") or "").strip(),
                ]
            )
            if value
        ],
    }
    return enriched


def get_journal_posts() -> List[Dict[str, Any]]:
    payload = load_journal_posts_payload()
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    seen = set()
    posts: List[Dict[str, Any]] = []
    for item in items:
        normalized = _normalize_post(item)
        if not normalized:
            continue
        slug = normalized["slug"]
        if slug in seen:
            continue
        seen.add(slug)
        posts.append(normalized)
    if not posts:
        save_json(JOURNAL_POSTS_FILE, _build_default_store())
        fallback_posts: List[Dict[str, Any]] = []
        for item in DEFAULT_JOURNAL_POSTS:
            normalized = _normalize_post(item)
            if normalized:
                fallback_posts.append(normalized)
        return fallback_posts
    return posts


def get_journal_post(slug: str) -> Optional[Dict[str, Any]]:
    slug_value = str(slug or "").strip()
    for post in get_journal_posts():
        if post["slug"] == slug_value:
            return post
    return None


def _build_category_list(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    categories = [{"key": "all", "label": "Все статьи", "count": len(posts)}]
    for key, meta in JOURNAL_CATEGORY_META.items():
        categories.append(
            {
                "key": key,
                "label": meta["label"],
                "count": sum(1 for post in posts if post["category"] == key),
            }
        )
    return categories


def build_posts_page_content() -> Dict[str, Any]:
    posts = get_journal_posts()
    return {
        "eyebrow": "Материалы и база знаний",
        "title": "Практические статьи, разборы и истории студентов Peaky Minds",
        "description": (
            "Здесь лежат разборы задач, инженерные заметки, карьерные материалы и истории людей, "
            "которые входят в IT не через лозунги, а через реальную практику."
        ),
        "categories": _build_category_list(posts),
        "featured": posts[0],
        "promo": {
            "kicker": "Профориентация",
            "title": "Не знаете, с чего стартовать в IT?",
            "text": (
                "Пройдите короткий разбор траектории. Покажем, какой стек вам ближе и как собрать "
                "первый внятный маршрут без лишней теории."
            ),
            "primary_cta": "Подобрать направление",
            "secondary_cta": "Открыть блог",
        },
        "posts": posts[1:],
    }


def build_blog_page_content() -> Dict[str, Any]:
    posts = get_journal_posts()
    by_slug = {post["slug"]: post for post in posts}
    featured = posts[1] if len(posts) > 1 else posts[0]
    story_slugs = [
        ("from-office-to-it-story", "Backend track", "#6d52ff"),
        ("sql-interview-task-breakdown", "Data track", "#4d7dff"),
        ("first-freelance-money", "Automation track", "#32d58a"),
        ("frontend-portfolio-package", "Design + Frontend", "#ff8e5c"),
    ]
    student_stories = []
    for slug, role, accent in story_slugs:
        post = by_slug.get(slug)
        if not post:
            continue
        student_stories.append(
            {
                "name": post["author"]["name"],
                "role": role,
                "title": post["title"],
                "excerpt": post["excerpt"],
                "date": post["date"],
                "initials": post["author"]["initials"],
                "accent": accent,
                "href": post["url"],
            }
        )

    return {
        "eyebrow": "Peaky Minds Journal",
        "title": "Журнал про обучение, карьеру, практику и переход в IT без маркетингового тумана",
        "description": (
            "Мы собираем материалы, которые помогают лучше понимать рынок, инструменты, реальные "
            "проекты и поведение на собеседованиях. Без инфошума и пустых обещаний."
        ),
        "stats": [
            {"value": f"{len(posts)}+", "label": "материалов в базе"},
            {"value": str(len(JOURNAL_CATEGORY_META)), "label": "основных траекторий"},
            {"value": "2 раза в неделю", "label": "новые публикации"},
        ],
        "categories": _build_category_list(posts)[1:],
        "featured": featured,
        "promo": {
            "title": "Откройте все материалы и фильтруйте по направлениям",
            "text": (
                "Если нужен быстрый вход в практические статьи и истории студентов, переходите в "
                "отдельный каталог материалов."
            ),
            "cta": "Перейти в посты",
            "href": "/posts",
        },
        "popular": [posts[0], posts[2], posts[4]] if len(posts) >= 5 else posts[:3],
        "student_stories": student_stories,
        "collections": [
            {
                "title": "Путь в backend",
                "count": "12 материалов",
                "text": "Python, Git, архитектура бэкенда, бот-проекты и подготовка к первой рабочей разработке.",
                "href": "/posts?category=programming",
            },
            {
                "title": "Data и аналитика",
                "count": "9 материалов",
                "text": "SQL, логика таблиц, интервью-задачи, аналитическое мышление и реальные запросы бизнеса.",
                "href": "/posts?category=analytics",
            },
            {
                "title": "Карьера и рост",
                "count": "11 материалов",
                "text": "Собеседования, pet-project, упаковка опыта, портфолио и первые проектные деньги.",
                "href": "/posts?category=career",
            },
        ],
        "latest": posts[3:9] if len(posts) >= 9 else posts[3:],
        "newsletter": {
            "title": "Получать новые материалы и разборы без шума",
            "text": (
                "Новые заметки, карьерные разборы и полезные материалы удобнее всего отдавать в "
                "Telegram. Это быстрее, чем ждать полноценный newsletter backend."
            ),
            "cta": "Открыть Telegram",
            "href": "https://t.me/IT_school_PM",
        },
    }


def build_post_detail_content(slug: str) -> Optional[Dict[str, Any]]:
    post = get_journal_post(slug)
    if not post:
        return None
    posts = get_journal_posts()
    same_category = [item for item in posts if item["slug"] != post["slug"] and item["category"] == post["category"]]
    fallback = [item for item in posts if item["slug"] != post["slug"] and item["category"] != post["category"]]
    related = (same_category + fallback)[:3]
    return {
        "post": post,
        "related": related,
        "breadcrumbs": [
            {"label": "Главная", "href": "/"},
            {"label": "Журнал", "href": "/blog"},
            {"label": "Материалы", "href": "/posts"},
            {"label": post["title"], "href": post["url"]},
        ],
    }


def build_journal_api_payload() -> Dict[str, Any]:
    posts = get_journal_posts()
    return {
        "ok": True,
        "version": load_journal_posts_payload().get("version", 1),
        "count": len(posts),
        "categories": _build_category_list(posts),
        "items": posts,
    }
