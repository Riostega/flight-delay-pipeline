"""Render the dashboard headlessly and fail if anything raises.

Streamlit's AppTest executes the whole script the way a real session would, so
this catches what a syntax check cannot: a renamed column, a type that will not
format, a query that no longer compiles. Both dashboard defects found during
development — Decimal refusing to multiply with a float, and a palette validated
against the wrong surface — were invisible to compileall and would have been
caught here.

Needs Snowflake credentials, so it runs in the workflow that has them.

    python3 dashboard/smoke_test.py
"""

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parent / "app.py"


def main() -> int:
    at = AppTest.from_file(str(APP), default_timeout=180)
    at.run()

    if at.exception:
        print(f"FAIL: {len(at.exception)} exception(s) while rendering")
        for e in at.exception:
            print(f"  {e.value[:400]}")
        return 1

    # A render with no exceptions but no content would also be a failure.
    if len(at.tabs) < 4:
        print(f"FAIL: expected 4 tabs, rendered {len(at.tabs)}")
        return 1
    if not at.metric:
        print("FAIL: rendered no metrics")
        return 1

    print(f"OK: {len(at.tabs)} tabs, {len(at.metric)} metrics, {len(at.dataframe)} tables")
    for m in at.metric:
        print(f"  {m.label}: {m.value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
