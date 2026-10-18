import os
from pathlib import Path
import shutil
import subprocess
import sys


def test_setup_creates_private_env_and_preserves_it_on_repeat(tmp_path):
    root = tmp_path / "checkout"
    (root / "scripts").mkdir(parents=True)
    original = Path(__file__).resolve().parent.parent
    for filename in (".env.example", "requirements-dev.txt", "requirements.txt"):
        shutil.copyfile(original / filename, root / filename)
    shutil.copyfile(original / "scripts/setup.sh", root / "scripts/setup.sh")
    fake = tmp_path / "bootstrap-python"
    fake.write_text(f'''#!{sys.executable}
import pathlib, subprocess, sys
if sys.argv[1:3] == ["-m", "venv"]:
    target = pathlib.Path(sys.argv[3]) / "bin/python"
    target.parent.mkdir(parents=True)
    target.write_text("#!/bin/sh\\nexit 0\\n")
    target.chmod(0o755)
else:
    raise SystemExit(subprocess.run([sys.executable] + sys.argv[1:]).returncode)
''')
    fake.chmod(0o755)
    env = dict(os.environ, PYTHON_BIN=str(fake))
    result = subprocess.run(["bash", str(root / "scripts/setup.sh")], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (root / ".env").stat().st_mode & 0o777 == 0o600
    (root / ".env").write_text("CUSTOM=preserved\n")
    result = subprocess.run(["bash", str(root / "scripts/setup.sh")], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (root / ".env").read_text() == "CUSTOM=preserved\n"
