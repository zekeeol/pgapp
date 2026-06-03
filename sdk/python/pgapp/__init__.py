from __future__ import annotations

from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

_generated_pgapp = Path(__file__).resolve().parent.parent / "pgapp_sdk" / "gen" / "pgapp"
if _generated_pgapp.is_dir():
    __path__.append(str(_generated_pgapp))

__all__: list[str] = []
