from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class ParsedURI:
    scheme: str
    path: str
    params: dict[str, str]


def parse(uri: str) -> ParsedURI:
    """Parse a DataIngest source/sink URI.

    Examples:
        csv:///data/clay.csv          -> scheme='csv', path='/data/clay.csv'
        sqlite:///./out.db            -> scheme='sqlite', path='./out.db'
        csv:///data/clay.csv?delim=;  -> scheme='csv', path='/data/clay.csv', params={'delim': ';'}
    """
    parsed = urlparse(uri)
    if not parsed.scheme:
        raise ValueError(f"URI missing scheme: {uri!r}")
    path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return ParsedURI(scheme=parsed.scheme, path=path, params=params)
