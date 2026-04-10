import sys
from pathlib import Path
import streamlit.web.cli as stcli
from argparse import ArgumentParser
from pipeline import run_pipeline

sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":

    parser = ArgumentParser(
        prog="GDELT-Analyzer",
        description="A tool for querying and analyzing GDELT event data"
    )
    parser.add_argument('-g', '--get', action='store_true', default=False, help="Fetch new data from GDELT database")
    parser.add_argument('-c', '--clear', action='store_true', default=False, help="Clear old files from the DB")
    parser.add_argument("-a", '--analyze', action='store_true', default=False, help="Perform analysis on downloaded data")
    parser.add_argument('-u', '--ui', action='store_true', default=False, help="Launch the UI")
    parser.add_argument('-H', '--hours', type=int, default=8, help="Time window in hours to download files from")
    parser.add_argument('-i', '--indexes', action="store_true", default=False, help="Create compound indexes for most queried fields")
   
    args = parser.parse_args()

    print("Welcome to GDELT-Analyzer!")

    if args.get and args.analyze:
        print("Starting pipeline (full)...\n")
        run_pipeline(args.hours, args.clear, True, True, args.indexes)

    elif args.get and not args.analyze:
        print("Starting pipeline (download only)...\n")
        run_pipeline(args.hours, args.clear, True, False, args.indexes)

    elif args.analyze and not args.get:
        print("Starting pipeline (analysis only)...\n")
        run_pipeline(args.hours, args.clear, False, True, args.indexes)

    elif not args.analyze and not args.get and args.indexes:
        print("Starting pipeline (indexing only)...")
        run_pipeline(args.hours, args.clear, False, False, args.indexes)
    else:
        print("Pipeline not launched. Specify flags to download and analyze files.")

    if args.ui:
        print("Starting UI...\n")
        stcli.main(["run", "app/GDELT-Analyzer.py", "--server.port=8501"])