# Wikipedia Revision Text Analysis with PySpark

This project analyzes large-scale Wikipedia revision data using Apache Spark. It focuses on measuring how article text changes over time through revision length, sentence-length consistency, and uncommon-word usage.

## Project Overview

The project processes JSON revision datasets stored in HDFS and generates aggregated outputs for later analysis or visualization. It was developed as a university big-data project using PySpark.

## Main Analyses

- **Revision text length over time**  
  Calculates total text length per revision/date pair.

- **Sentence consistency analysis**  
  Splits revision text into sentences, calculates sentence word counts, and measures variance in sentence length.

- **Uncommon word analysis**  
  Compares revision text against a list of common ChatGPT-style words and counts uncommon words per revision.

- **Monthly aggregation**  
  Aggregates uncommon-word counts by year and month to support trend analysis.

## Repository Structure

```text
.
├── pySparkRevisions.py       # Computes revision text length metrics
├── SentenceConsistency.py    # Computes sentence-length variance metrics
├── UconnomWords1.py          # Counts uncommon words per revision
├── UncommonWords2.py         # Aggregates uncommon-word counts by month
└── README.md
```

## Technologies Used

- Python
- PySpark
- Apache Spark SQL
- HDFS
- JSON / CSV data processing

## Data Inputs

The scripts expect Wikipedia revision data stored in HDFS using paths similar to:

```text
/user/<username>/SHARED_MBD/rev_data/
```

Expected files include:

```text
<title>_revisions.json
<title>_revision_content.json
```

The uncommon-word analysis also expects a local/common-word CSV file:

```text
combined_chatgpt_words.csv
```

## How to Run

Run each script with `spark-submit`:

```bash
spark-submit pySparkRevisions.py
spark-submit SentenceConsistency.py
spark-submit UncommonWords1.py
spark-submit UncommonWords2.py
```


## Outputs

The scripts write processed outputs back to HDFS, including:

```text
rev_data_csv/
rev_data_SenLen/
outputUncommon/
outputUncommon/aggregated_growth.json
```

These outputs can be used for downstream plotting, trend analysis, or reporting.

## Key Skills Demonstrated

- Large-scale data processing with PySpark
- Working with distributed storage using HDFS
- Cleaning and transforming semi-structured JSON data
- Building Spark SQL DataFrame pipelines
- Aggregating temporal text metrics
- Designing reproducible big-data analysis workflows

## Suggested Improvements

Planned improvements before or after publication:

- Replace hardcoded HDFS paths with command-line arguments
- Rename files using consistent naming conventions
- Add sample input schema and small demo dataset
- Add a `requirements.txt` or environment setup instructions
- Add plots or screenshots showing final trends
- Add more robust text preprocessing and tokenization

## Academic Context

This project was completed as part of university coursework involving big-data processing and text analysis.
