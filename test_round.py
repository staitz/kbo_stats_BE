from typing import Any
import sys

def test() -> None:
    h: int = 10
    bb: int = 5
    ab: int = 30
    tb: int = 20

    avg = float(h / ab) if ab > 0 else 0.0
    obp = float(h + bb) / (ab + bb) if (ab + bb) > 0 else 0.0
    slg = float(tb / ab) if ab > 0 else 0.0

    round(avg, 4)
    round(obp, 4)
    round(slg, 4)
    round(obp + slg, 4)

