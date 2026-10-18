"""Run the source checkout without requiring package installation."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from commerce_etl.cli import main

if __name__ == "__main__":
    main()
