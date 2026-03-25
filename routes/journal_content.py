from typing import Any, Dict, List


JOURNAL_CATEGORY_META: Dict[str, Dict[str, str]] = {
    "programming": {"label": "Программирование"},
    "analytics": {"label": "Аналитика"},
    "automation": {"label": "Автоматизация"},
    "design": {"label": "Дизайн"},
    "career": {"label": "Карьера"},
    "stories": {"label": "Истории студентов"},
}


JOURNAL_POSTS: List[Dict[str, Any]] = [
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


def _build_post(post: Dict[str, Any]) -> Dict[str, Any]:
    category_meta = JOURNAL_CATEGORY_META[post["category"]]
    return {
        **post,
        "category_label": category_meta["label"],
        "url": f"/posts#{post['slug']}",
    }


ENRICHED_JOURNAL_POSTS: List[Dict[str, Any]] = [_build_post(post) for post in JOURNAL_POSTS]
JOURNAL_CATEGORY_KEYS = {"all", *JOURNAL_CATEGORY_META.keys()}


def _build_category_list() -> List[Dict[str, Any]]:
    categories = [{"key": "all", "label": "Все статьи", "count": len(ENRICHED_JOURNAL_POSTS)}]
    for key, meta in JOURNAL_CATEGORY_META.items():
        categories.append(
            {
                "key": key,
                "label": meta["label"],
                "count": sum(1 for post in ENRICHED_JOURNAL_POSTS if post["category"] == key),
            }
        )
    return categories


POSTS_PAGE_CONTENT: Dict[str, Any] = {
    "eyebrow": "Материалы и база знаний",
    "title": "Практические статьи, разборы и истории студентов Peaky Minds",
    "description": (
        "Здесь лежат разборы задач, инженерные заметки, карьерные материалы и истории людей, "
        "которые входят в IT не через лозунги, а через реальную практику."
    ),
    "categories": _build_category_list(),
    "featured": ENRICHED_JOURNAL_POSTS[0],
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
    "posts": ENRICHED_JOURNAL_POSTS[1:],
}


BLOG_PAGE_CONTENT: Dict[str, Any] = {
    "eyebrow": "Peaky Minds Journal",
    "title": "Журнал про обучение, карьеру, практику и переход в IT без маркетингового тумана",
    "description": (
        "Мы собираем материалы, которые помогают лучше понимать рынок, инструменты, реальные "
        "проекты и поведение на собеседованиях. Без инфошума и пустых обещаний."
    ),
    "stats": [
        {"value": "120+", "label": "материалов в базе"},
        {"value": "6", "label": "основных траекторий"},
        {"value": "2 раза в неделю", "label": "новые публикации"},
    ],
    "categories": _build_category_list()[1:],
    "featured": ENRICHED_JOURNAL_POSTS[1],
    "promo": {
        "title": "Откройте все материалы и фильтруйте по направлениям",
        "text": (
            "Если нужен быстрый вход в практические статьи и истории студентов, переходите в "
            "отдельный каталог материалов."
        ),
        "cta": "Перейти в посты",
        "href": "/posts",
    },
    "popular": [
        ENRICHED_JOURNAL_POSTS[0],
        ENRICHED_JOURNAL_POSTS[2],
        ENRICHED_JOURNAL_POSTS[4],
    ],
    "student_stories": [
        {
            "name": "Илья Морозов",
            "role": "Backend track",
            "title": "Из офиса в backend-практику за 8 месяцев",
            "excerpt": "Как он прошёл путь от полного перегруза к первым интервью и рабочему проекту.",
            "date": "02 марта 2026",
            "initials": "ИМ",
            "accent": "#6d52ff",
            "href": "/posts?category=stories#from-office-to-it-story",
        },
        {
            "name": "Алина Ким",
            "role": "Data track",
            "title": "Как перестать бояться SQL и начать решать задачи бизнеса",
            "excerpt": "Разбор трека аналитики глазами человека без технического бэкграунда.",
            "date": "27 февраля 2026",
            "initials": "АК",
            "accent": "#4d7dff",
            "href": "/posts?category=analytics#sql-interview-task-breakdown",
        },
        {
            "name": "Даниил Петров",
            "role": "Automation track",
            "title": "Первый платный automation-кейс для локального бизнеса",
            "excerpt": "Что помогло продать решение без команды и без ощущения, что ты ещё не готов.",
            "date": "23 февраля 2026",
            "initials": "ДП",
            "accent": "#32d58a",
            "href": "/posts?category=stories#first-freelance-money",
        },
        {
            "name": "Марина Шевцова",
            "role": "Design + Frontend",
            "title": "Как упаковать портфолио, если коммерческих кейсов ещё мало",
            "excerpt": "Не ждать идеального проекта, а показать мышление, качество и логику решения.",
            "date": "08 февраля 2026",
            "initials": "МШ",
            "accent": "#ff8e5c",
            "href": "/posts?category=design#frontend-portfolio-package",
        },
    ],
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
    "latest": [
        ENRICHED_JOURNAL_POSTS[3],
        ENRICHED_JOURNAL_POSTS[5],
        ENRICHED_JOURNAL_POSTS[7],
        ENRICHED_JOURNAL_POSTS[8],
        ENRICHED_JOURNAL_POSTS[9],
        ENRICHED_JOURNAL_POSTS[10],
    ],
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
