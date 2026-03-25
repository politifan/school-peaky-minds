from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from core import ASSETS_DIR, render
from routes.course_content import COURSE_PAGES
from routes.homepage_content import CALENDAR_PREVIEW

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
            "course": COURSE_PAGES[course_key],
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
        },
    )


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
        ("courses/fullstack", "0.85"),
        ("courses/data-science", "0.85"),
        ("courses/business", "0.85"),
        ("courses/python-beginners", "0.85"),
    ]
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
