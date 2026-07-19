"""Tests for the static map renderer."""

from io import BytesIO
from pathlib import Path
from urllib.request import Request

from PIL import Image
import pytest

from pymammotion.utility import map_renderer
from pymammotion.utility.map_renderer import ESRI_WORLD_IMAGERY_TILE_PROVIDER, OPENSTREETMAP_TILE_PROVIDER


def test_openstreetmap_tile_url() -> None:
    """OpenStreetMap uses standard XYZ tile ordering."""
    assert (  # noqa: S101
        OPENSTREETMAP_TILE_PROVIDER.tile_url(19, 123, 456) == "https://tile.openstreetmap.org/19/123/456.png"
    )


def test_esri_world_imagery_tile_url() -> None:
    """Esri World Imagery uses z/y/x tile ordering."""
    assert ESRI_WORLD_IMAGERY_TILE_PROVIDER.tile_url(  # noqa: S101
        19, 123, 456
    ).endswith("/tile/19/456/123")


def test_tile_cache_is_partitioned_by_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Providers do not read tiles cached by another provider."""

    def tile_bytes(color: str) -> bytes:
        buffer = BytesIO()
        Image.new("RGB", (1, 1), color).save(buffer, format="PNG")
        return buffer.getvalue()

    def fake_urlopen(request: Request, timeout: float) -> BytesIO:
        del timeout
        color = "blue" if "arcgisonline" in request.full_url else "red"
        return BytesIO(tile_bytes(color))

    monkeypatch.setattr(map_renderer, "urlopen", fake_urlopen)

    osm_tile = map_renderer._load_tile(  # noqa: SLF001
        19, 123, 456, str(tmp_path), OPENSTREETMAP_TILE_PROVIDER
    )
    satellite_tile = map_renderer._load_tile(  # noqa: SLF001
        19, 123, 456, str(tmp_path), ESRI_WORLD_IMAGERY_TILE_PROVIDER
    )

    assert osm_tile is not None  # noqa: S101
    assert satellite_tile is not None  # noqa: S101
    assert osm_tile.getpixel((0, 0)) == (255, 0, 0)  # noqa: S101
    assert satellite_tile.getpixel((0, 0)) == (0, 0, 255)  # noqa: S101
    assert (  # noqa: S101
        tmp_path / "openstreetmap" / "19" / "123" / "456.png"
    ).is_file()
    assert (  # noqa: S101
        tmp_path / "esri_world_imagery" / "19" / "123" / "456.png"
    ).is_file()


def test_openstreetmap_uses_legacy_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Existing unpartitioned OpenStreetMap caches remain usable."""
    legacy_cache = tmp_path / "19" / "123" / "456.png"
    legacy_cache.parent.mkdir(parents=True)
    Image.new("RGB", (1, 1), "red").save(legacy_cache, format="PNG")

    def fail_urlopen(request: Request, timeout: float) -> BytesIO:
        del request, timeout
        pytest.fail("the legacy cached tile should avoid a network request")

    monkeypatch.setattr(map_renderer, "urlopen", fail_urlopen)

    tile = map_renderer._load_tile(  # noqa: SLF001
        19, 123, 456, str(tmp_path), OPENSTREETMAP_TILE_PROVIDER
    )

    assert tile is not None  # noqa: S101
    assert tile.getpixel((0, 0)) == (255, 0, 0)  # noqa: S101
