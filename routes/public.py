from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from core import ASSETS_DIR, render
from routes.journal_content import (
    JOURNAL_CATEGORY_KEYS,
    build_blog_page_content,
    build_journal_api_payload,
    build_post_detail_content,
    build_posts_page_content,
    get_journal_posts,
)
from routes.course_content import COURSE_PAGES, HOME_COURSE_KEYS, build_course_catalog_content
from routes.homepage_content import CALENDAR_PREVIEW
from routes.marketing_content import build_homepage_marketing, decorate_catalog_page, decorate_course

router = APIRouter()


def _load_amp_css() -> str:
    css_path = ASSETS_DIR / "css" / "styles.css"
    try:
        return css_path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _render_course(request: Request, course_key: str, course_slug: str):
    return render(
        request,
        "course_marketing.html",
        {
            "course": decorate_course(course_key, COURSE_PAGES[course_key]),
            "course_slug": course_slug,
            "amp_css": _load_amp_css(),
        },
    )


@router.get("/", include_in_schema=False)
async def index(request: Request):
    return render(
        request,
        "index.html",
        {
            "amp_css": _load_amp_css(),
            "calendar_preview": CALENDAR_PREVIEW,
            "home_courses": [decorate_course(key, COURSE_PAGES[key]) for key in HOME_COURSE_KEYS],
            "marketing": build_homepage_marketing(),
        },
    )


@router.get("/index.html", include_in_schema=False)
async def index_alias(request: Request):
    return render(
        request,
        "index.html",
        {
            "amp_css": _load_amp_css(),
            "calendar_preview": CALENDAR_PREVIEW,
            "home_courses": [decorate_course(key, COURSE_PAGES[key]) for key in HOME_COURSE_KEYS],
            "marketing": build_homepage_marketing(),
        },
    )


@router.get("/posts", include_in_schema=False)
@router.get("/posts/", include_in_schema=False)
async def posts(request: Request):
    selected_category = request.query_params.get("category", "all")
    if selected_category not in JOURNAL_CATEGORY_KEYS:
        selected_category = "all"

    return render(
        request,
        "posts.html",
        {
            "amp_css": _load_amp_css(),
            "posts_page": build_posts_page_content(),
            "selected_posts_category": selected_category,
        },
    )


@router.get("/posts/{slug}", include_in_schema=False)
async def post_detail(request: Request, slug: str):
    post_page = build_post_detail_content(slug)
    if not post_page:
        response = render(request, "404.html", {"amp_css": _load_amp_css()})
        response.status_code = 404
        return response
    return render(
        request,
        "post_detail.html",
        {
            "amp_css": _load_amp_css(),
            "post_page": post_page,
        },
    )


@router.get("/blog", include_in_schema=False)
@router.get("/blog/", include_in_schema=False)
async def blog(request: Request):
    return render(
        request,
        "blog.html",
        {
            "amp_css": _load_amp_css(),
            "blog_page": build_blog_page_content(),
        },
    )


@router.get("/api/journal/posts", include_in_schema=False)
async def journal_posts_api():
    return JSONResponse(build_journal_api_payload())


@router.get("/api/journal/posts/{slug}", include_in_schema=False)
async def journal_post_api(slug: str):
    post_page = build_post_detail_content(slug)
    if not post_page:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    return JSONResponse({"ok": True, "item": post_page["post"]})


@router.get("/robots.txt", include_in_schema=False)
async def robots(request: Request):
    base_url = str(request.base_url)
    content = f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml\n"
    return PlainTextResponse(content, media_type="text/plain")


@router.get("/favicon.ico", include_in_schema=False)
async def favicon() -> RedirectResponse:
    return RedirectResponse("/assets/img/favicon.ico", status_code=HTTP_302_FOUND)


@router.get("/sitemap.xml", include_in_schema=False)
async def sitemap(request: Request):
    base_url = str(request.base_url)
    lastmod = date.today().isoformat()
    urls = [
        ("", "1.0"),
        ("blog", "0.85"),
        ("posts", "0.8"),
        ("courses", "0.9"),
    ]
    urls.extend((course["path"].lstrip("/"), "0.85") for course in COURSE_PAGES.values())
    urls.extend((f"posts/{post['slug']}", "0.72") for post in get_journal_posts())
    entries = "\n".join(
        [
            "  <url>"
            f"<loc>{base_url}{path}</loc>"
            f"<lastmod>{lastmod}</lastmod>"
            f"<changefreq>weekly</changefreq>"
            f"<priority>{priority}</priority>"
            "</url>"
            for path, priority in urls
        ]
    )
    xml = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">
{entries}
</urlset>"""
    return Response(content=xml, media_type="application/xml")


@router.get("/course-fullstack.html", include_in_schema=False)
async def course_fullstack_legacy(request: Request):
    return RedirectResponse("/courses/fullstack", status_code=HTTP_302_FOUND)


@router.get("/courses", include_in_schema=False)
@router.get("/courses/", include_in_schema=False)
async def courses_catalog(request: Request):
    return render(
        request,
        "courses_catalog.html",
        {
            "amp_css": _load_amp_css(),
            "courses_page": decorate_catalog_page(build_course_catalog_content()),
        },
    )


@router.get("/courses/fullstack", include_in_schema=False)
@router.get("/courses/fullstack/", include_in_schema=False)
async def course_fullstack(request: Request):
    return _render_course(request, "fullstack", "Full-stack")


@router.get("/course-datascience.html", include_in_schema=False)
async def course_datascience_legacy(request: Request):
    return RedirectResponse("/courses/data-science", status_code=HTTP_302_FOUND)


@router.get("/courses/data-science", include_in_schema=False)
@router.get("/courses/data-science/", include_in_schema=False)
async def course_datascience(request: Request):
    return _render_course(request, "data_science", "Data Science")


@router.get("/course-business.html", include_in_schema=False)
async def course_business_legacy(request: Request):
    return RedirectResponse("/courses/business", status_code=HTTP_302_FOUND)


@router.get("/course-python-beginners.html", include_in_schema=False)
async def course_python_beginners_legacy(request: Request):
    return RedirectResponse("/courses/python-beginners", status_code=HTTP_302_FOUND)


@router.get("/courses/business", include_in_schema=False)
@router.get("/courses/business/", include_in_schema=False)
async def course_business(request: Request):
    return _render_course(request, "business", "Business")


@router.get("/courses/python-beginners", include_in_schema=False)
@router.get("/courses/python-beginners/", include_in_schema=False)
async def course_python_beginners(request: Request):
    return _render_course(request, "python_start", "Python для новичков")


@router.get("/courses/{slug}", include_in_schema=False)
@router.get("/courses/{slug}/", include_in_schema=False)
async def course_dynamic(request: Request, slug: str):
    for course_key, course in COURSE_PAGES.items():
        if course["path"].rstrip("/").endswith("/" + slug):
            return _render_course(request, course_key, course["name"])
    response = render(request, "404.html", {"amp_css": _load_amp_css()})
    response.status_code = 404
    return response


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict:
    return {"status": "ok"}


@router.get("/429", include_in_schema=False)
async def too_many_requests(request: Request):
    response = render(
        request,
        "enroll_limit.html",
        {
            "limit_title": "Запросы временно ограничены",
            "limit_message": "Мы заметили подозрительный трафик, исходящий из вашей сети. Попробуйте оставить заявку позднее.",
            "limit_hint": "Если это ошибка, просто повторите попытку чуть позже.",
        },
    )
    response.status_code = 429
    return response
