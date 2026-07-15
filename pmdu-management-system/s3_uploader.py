import base64
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import boto3
import requests
from lxml import html as lh

LOGGER_NAME = "pmdu_s3_uploader"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "admin"),
        region_name="us-east-1",
    )


def fetch_resource(
    url: str, session: requests.Session, timeout: int = 30
) -> bytes | None:
    """Download a resource from PMDU."""
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        print(f"Failed to fetch {url}: {exc}")
        return None


def to_data_uri(data: bytes, mime_type: str) -> str:
    """Convert bytes to base64 data URI."""
    b64 = base64.b64encode(data).decode("ascii")
    return f"data:{mime_type};base64,{b64}"


def guess_mime_type(path: str) -> str:
    """Guess MIME type from file extension."""
    ext = Path(path).suffix.lower()
    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".css": "text/css",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".ttf": "font/ttf",
        ".otf": "font/otf",
        ".eot": "application/vnd.ms-fontobject",
    }
    return mime_map.get(ext, "application/octet-stream")


def inline_css_resources(
    css_text: str, css_url: str, base_url: str, session: requests.Session
) -> str:
    """
    Find all url(...) in CSS, download them, replace with data URIs.
    """
    url_pattern = re.compile(r'url\(\s*["\']?([^"\'()\s]+)["\']?\s*\)')

    def replace_url(match):
        resource_url = match.group(1)
        if resource_url.startswith("data:") or resource_url.startswith("http"):
            return match.group(0)

        full_url = urljoin(urljoin(base_url, css_url), resource_url)
        data = fetch_resource(full_url, session)
        if data:
            mime = guess_mime_type(full_url)
            return f'url("{to_data_uri(data, mime)}")'
        return match.group(0)

    return url_pattern.sub(replace_url, css_text)


def create_self_contained_html(
    html_content: str, page_url: str, session: requests.Session
) -> str:
    """
    Create fully self-contained HTML with all CSS and resources inlined.
    """
    doc = lh.fromstring(html_content)

    # 1. Process CSS links — download and inline as <style> tags
    css_links = doc.xpath("//link[@rel='stylesheet']")
    for link in css_links:
        href = link.get("href", "")
        if not href:
            link.getparent().remove(link)
            continue

        css_url = urljoin(page_url, href)
        css_data = fetch_resource(css_url, session)
        if css_data:
            css_text = css_data.decode("utf-8", errors="replace")
            # Inline any resources referenced in the CSS
            css_text = inline_css_resources(css_text, href, page_url, session)

            # Replace <link> with <style>
            style_elem = lh.Element("style")
            style_elem.text = css_text
            link.addprevious(style_elem)

        link.getparent().remove(link)

    # 2. Process <img> tags — convert to base64 data URIs
    images = doc.xpath("//img[@src]")
    for img in images:
        src = img.get("src", "")
        if not src or src.startswith("data:"):
            continue

        img_url = urljoin(page_url, src)
        img_data = fetch_resource(img_url, session)
        if img_data:
            mime = guess_mime_type(img_url)
            img.set("src", to_data_uri(img_data, mime))

    # 3. Process inline styles with url(...) — background images etc.
    for elem in doc.xpath("//*[@style]"):
        style = elem.get("style", "")
        if "url(" in style:
            new_style = inline_css_resources(style, "", page_url, session)
            elem.set("style", new_style)

    # 4. Add print CSS
    head = doc.xpath("//head")[0] if doc.xpath("//head") else None
    if head is None:
        html_elem = doc.xpath("//html")[0]
        head = lh.Element("head")
        html_elem.insert(0, head)

    print_style = lh.Element("style")
    print_style.text = """
        @page { size: A4; margin: 10mm; }
        @media print {
            body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
            .no-print, .btn, .dataTables_paginate, .dataTables_length, .dataTables_filter { display: none !important; }
        }
    """
    head.append(print_style)

    # Return full self-contained HTML
    return lh.tostring(
        doc, encoding="unicode", method="html", doctype="<!DOCTYPE html>"
    )


def upload_complaint_package(
    complaint_code: str, html_content: str, page_url: str, bucket: str = "complaints"
):
    """
    Upload self-contained HTML to complaint-specific folder.
    Structure: sources/pmdu/{complaint_code}/raw.html
    """
    s3 = get_s3_client()
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    )

    # Create self-contained HTML
    self_contained = create_self_contained_html(html_content, page_url, session)

    # Upload to complaint-specific folder
    key = f"sources/pmdu/{complaint_code}/raw.html"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=self_contained.encode("utf-8"),
        ContentType="text/html",
    )
    print(
        f"Uploaded self-contained HTML to s3://{bucket}/{key} ({len(self_contained)} bytes)"
    )
    return key
