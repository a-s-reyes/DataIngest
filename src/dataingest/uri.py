import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

_WIN_DRIVE_PREFIX = re.compile(r"^/[A-Za-z]:[/\\]")


@dataclass(frozen=True)
class ParsedURI:
    scheme: str
    path: str
    params: dict[str, str]


def parse(uri: str) -> ParsedURI:
    parsed = urlparse(uri)
    if not parsed.scheme:
        raise ValueError(f"URI missing scheme: {uri!r}")
    path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return ParsedURI(scheme=parsed.scheme, path=path, params=params)


def resolve_uri_path(uri_path: str) -> str:
    if _WIN_DRIVE_PREFIX.match(uri_path):
        return uri_path[1:]
    if uri_path.startswith("/./"):
        return uri_path[1:]
    if uri_path.startswith("//"):
        return uri_path[1:]
    return uri_path
