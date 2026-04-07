import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    import streamlit.web.cli as stcli
    stcli.main(["run", "app/GDELT-Analyzer.py", "--server.port=8501"])