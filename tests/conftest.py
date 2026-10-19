import sys
from pathlib import Path

import pytest

# The pipeline modules import each other as `pipeline.*`, from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# If the watchdog's shared modules don't import, importing pipeline.watchdog runs its
# import-failure alarm, which reads the real .env, posts to the real Slack channel and
# fails the real heartbeat. Import them first, so a broken environment stops the run
# here instead of raising a false alarm.
try:
    import requests  # noqa: F401
    import pipeline.notify  # noqa: F401
    import pipeline.thresholds  # noqa: F401
except Exception as exc:
    pytest.exit(f"the watchdog tests need the pipeline's dependencies "
                f"(pip install -r requirements.txt): {exc!r}", returncode=2)
