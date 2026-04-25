from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def clay_csv(fixtures_dir: Path) -> Path:
    return fixtures_dir / "clay_sample.csv"


@pytest.fixture
def clay_mapping(tmp_path: Path) -> Path:
    src = PROJECT_ROOT / "mappings" / "clay.yml"
    dst = tmp_path / "clay.yml"
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return dst
