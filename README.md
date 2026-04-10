# GDELT-Analyzer

A data pipeline and analytics dashboard that ingests event data from the [GDELT Project](https://www.gdeltproject.org/), processes it with Apache Spark, stores results in MongoDB, and surfaces insights through an interactive Streamlit dashboard. This is a university project.

## Stack

- **Apache Spark** — distributed data ingestion and analysis
- **MongoDB** — storage for processed event collections
- **Streamlit** — interactive analytics dashboard
- **Python** — pipeline orchestration and utilities

## Project Structure

```
GDELT-Analyzer/
├── run.py              # Entry point (CLI)
├── pipeline.py         # Core pipeline logic
├── app/
│   ├── GDELT-Analyzer.py       # Streamlit home page
│   └── pages/
│       ├── 1_Global_Trends.py
│       ├── 2_Country_Insights.py
│       └── 3_Event_Spikes.py
├── core/
│   ├── gdelt_analysis.py   # Spark analysis functions
│   ├── spark_ingest.py     # Data ingestion
│   ├── mongo_utils.py      # MongoDB helpers
│   └── queries.py          # Query definitions
└── utils/
    ├── download_utils.py
    └── file_utils.py
```

## Prerequisites

- Python 3.9+
- Java 8 or 11 (required by Apache Spark)
- MongoDB running locally on `mongodb://127.0.0.1:27017/`
- Apache Spark (managed via PySpark)

## Setup

**1. Clone the repository**

```bash
git clone git@github.com:AleksiKupila/GDELT-Analyzer.git
cd GDELT-Analyzer
```

**2. Create and activate a virtual environment**

```bash
python -m venv venv
source venv/bin/activate       # Linux / macOS
# venv\Scripts\activate        # Windows
```

**3. Install dependencies**

```bash
pip install pyspark pymongo streamlit
```

## Usage

All functionality is accessed through `run.py`. Flags can be combined freely.

| Flag | Description |
|------|-------------|
| `-g` / `--get` | Download new data from GDELT |
| `-a` / `--analyze` | Run Spark analysis on stored data |
| `-u` / `--ui` | Launch the Streamlit dashboard |
| `-c` / `--clear` | Clear existing data and collections before running |
| `-H` / `--hours` | Time window in hours to fetch data from (default: 168 hours/7 days) |
| `-i` / `--indexes` | Create compound indexes on MongoDB collections |

**Download and analyze data, then launch the UI:**

```bash
python run.py -g -a -u
```

**Analyze already-downloaded data and open the dashboard:**

```bash
python run.py -a -u
```

**Full refresh (clear old data, download 200 hours, analyze, index, and launch UI):**

```bash
python run.py -c -g -a -i -u -H 200
```

## Dashboard Pages

- **Global Trends** — worldwide event heatmap, top-event rankings, and tone analysis by country
- **Country Insights** — drill-down filtering by country, actor, date range, Goldstein scale, and tone
- **Event Spikes** — detects countries with abnormal surges in reporting compared to a rolling baseline

## Data Source

All data is sourced from the [GDELT Project](https://www.gdeltproject.org/), an open real-time dataset of global events updated every 15 minutes and freely available under an open license.
