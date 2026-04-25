import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

# Matches "/<drive-letter>:<sep>" — e.g. /C:/foo, /c:\foo. Used to detect when
# urlparse has prepended a slash to a Windows absolute path.
_WIN_DRIVE_PREFIX = re.compile(r"^/[A-Za-z]:[/\\]")


@dataclass(frozen=True)
class ParsedURI:
    scheme: str
    path: str
    params: dict[str, str]


def parse(uri: str) -> ParsedURI:
    """Parse a DataIngest source/sink URI.

    Examples:
        csv:///data/file.csv          -> scheme='csv', path='/data/file.csv'
        sqlite:///./out.db            -> scheme='sqlite', path='./out.db'
        csv:///data/file.csv?delim=;  -> scheme='csv', path='/data/file.csv', params={'delim': ';'}
    """
    parsed = urlparse(uri)
    if not parsed.scheme:
        raise ValueError(f"URI missing scheme: {uri!r}")
    path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return ParsedURI(scheme=parsed.scheme, path=path, params=params)


def resolve_uri_path(uri_path: str) -> str:
    """Turn a parsed URI path into a usable filesystem path string.

    Handles the four shapes a DataIngest URI can produce, following
    SQLAlchemy's three-vs-four-slash convention:

      ``/C:/data/x.csv`` (Windows absolute) -> ``C:/data/x.csv``
      ``//tmp/x.csv``    (POSIX absolute)   -> ``/tmp/x.csv``
      ``/./x.csv``       (relative)          -> ``./x.csv``
      ``/some/path``     (POSIX absolute)   -> ``/some/path``  (passthrough)

    This is the single source of truth for URI -> filesystem-path conversion;
    every Source and Sink should call it rather than reimplementing.
    """
    if _WIN_DRIVE_PREFIX.match(uri_path):
        return uri_path[1:]
    if uri_path.startswith("/./"):
        return uri_path[1:]
    if uri_path.startswith("//"):
        return uri_path[1:]
    return uri_path
