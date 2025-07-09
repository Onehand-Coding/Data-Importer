import sys
import subprocess
from pathlib import Path


def main():
    script_path = Path(__file__).parent / "app.py"
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(script_path)], check=True
        )
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"[x] Streamlit exited with error code {e.returncode}")
        sys.exit(e.returncode)
