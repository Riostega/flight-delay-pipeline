import sys
from pathlib import Path

# The pipeline modules import each other as `pipeline.*`, from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
