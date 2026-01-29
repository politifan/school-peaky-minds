from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from core import ASSETS_DIR, render

router = APIRouter()


def _load_amp_css() -> str:
    css_path = ASSETS_DIR / "css" / "styles.css"
    try:
        return css_path.read_text(encoding="utf-8")
    except Exception:
        return ""


@router.get("/", include_in_schema=False)
def index(request: Request):
    return render(request, "index.html", {"amp_css": _load_amp_css()})


@router.get("/index.html", include_in_schema=False)
def index_alias(request: Request):
    return render(request, "index.html", {"amp_css": _load_amp_css()})


@router.get("/robots.txt", include_in_schema=False)
def robots(request: Request):
    base_url = str(request.base_url)
    content = f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml\n"
    return PlainTextResponse(content, media_type="text/plain")


@router.get("/sitemap.xml", include_in_schema=False)
def sitemap(request: Request):
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
def course_fullstack_legacy(request: Request):
    return RedirectResponse("/courses/fullstack", status_code=HTTP_302_FOUND)


@router.get("/courses/fullstack", include_in_schema=False)
@router.get("/courses/fullstack/", include_in_schema=False)
def course_fullstack(request: Request):
    return render(
        request,
        "course-fullstack.html",
        {"course_slug": "Full-stack", "amp_css": _load_amp_css()},
    )


@router.get("/course-datascience.html", include_in_schema=False)
def course_datascience_legacy(request: Request):
    return RedirectResponse("/courses/data-science", status_code=HTTP_302_FOUND)


@router.get("/courses/data-science", include_in_schema=False)
@router.get("/courses/data-science/", include_in_schema=False)
def course_datascience(request: Request):
    return render(
        request,
        "course-datascience.html",
        {"course_slug": "Data Science", "amp_css": _load_amp_css()},
    )


@router.get("/course-business.html", include_in_schema=False)
def course_business_legacy(request: Request):
    return RedirectResponse("/courses/business", status_code=HTTP_302_FOUND)


@router.get("/course-python-beginners.html", include_in_schema=False)
def course_python_beginners_legacy(request: Request):
    return RedirectResponse("/courses/python-beginners", status_code=HTTP_302_FOUND)


@router.get("/courses/business", include_in_schema=False)
@router.get("/courses/business/", include_in_schema=False)
def course_business(request: Request):
    return render(
        request,
        "course-business.html",
        {"course_slug": "Business", "amp_css": _load_amp_css()},
    )


@router.get("/courses/python-beginners", include_in_schema=False)
@router.get("/courses/python-beginners/", include_in_schema=False)
def course_python_beginners(request: Request):
    return render(
        request,
        "course-python-beginners.html",
        {"course_slug": "Python для новичков", "amp_css": _load_amp_css()},
    )


@router.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok"}
