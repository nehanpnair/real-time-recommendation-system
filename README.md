# StreamRec

StreamRec is a real-time machine learning recommendation system built around Kafka event streaming, Spark Structured Streaming, Parquet, and ALS collaborative filtering.

The project demonstrates an end-to-end recommendation workflow: synthetic user activity is published to Kafka, consumed and aggregated with event-time semantics, persisted to Parquet, and used to train and evaluate a recommendation model. Development and execution are local; this repository is not a multi-node production deployment.

## Project Overview

StreamRec processes user-item interaction events such as views, clicks, searches, add-to-cart actions, and purchases. The streaming layer produces windowed user features and persists raw interaction records for downstream machine learning.

The current recommendation model is Spark MLlib ALS collaborative filtering. The synthetic dataset is intended to demonstrate the complete streaming and ML pipeline rather than represent production traffic or catalog behavior.

## Key Features

- Kafka topic `user-events` with three partitions.
- User ID partition keys for user-affine event distribution.
- Python Kafka producer using `confluent-kafka` and JSON serialization.
- Consumer groups, offsets, partition-aware consumption, and rebalancing.
- Stateful per-user aggregation with checkpointing and recovery.
- Event-time processing with five-minute windows, one-minute slides, and watermarks for late events.
- Spark Structured Streaming from Kafka to Parquet.
- Separate Parquet output for streaming features and interaction data.
- Weighted, bounded user-item ratings for ALS collaborative filtering.
- Train, validation, and test evaluation with RMSE and ranking metrics.
- Top-five recommendations with training-seen items excluded.

## Technologies

- Apache Kafka 4.2.1
- Apache Spark 4.2.0
- PySpark MLlib ALS
- Python 3.10+
- `confluent-kafka`
- `python-dotenv`
- Parquet

## Project Structure

```text
streamrec/
|-- README.md
|-- requirements.txt
|-- producer/
|   |-- config.py              Kafka producer configuration
|   |-- events.py              Synthetic event generation
|   `-- main.py                Publish events to Kafka
|-- consumer/
|   |-- config.py              Kafka consumer configuration
|   |-- main.py                Stateful Kafka consumer
|   |-- state.py               Per-user event-time state
|   |-- checkpoint.py          Checkpoint persistence and recovery
|   `-- checkpoints/
|       `-- state.json         Consumer checkpoint data
|-- spark/
|   `-- streaming_features.py  Kafka-to-Spark streaming job
|-- ml/
|   `-- train.py               ALS training and evaluation
|-- features/                  Reserved feature workspace
|-- experiments/               Reserved experiment workspace
|-- docs/                      Supporting documentation
`-- serving/                   Reserved serving workspace
```

## Data and Event Model

Synthetic events contain user, item, event, device, country, session, and timestamp fields. The generator creates activity for 100 users and 500 items. Users select preferred items most of the time and occasionally explore other catalog items.

Supported event types and model weights are:

| Event type | Weight |
| --- | ---: |
| `view` | 1 |
| `click` | 2 |
| `search` | 2 |
| `add_to_cart` | 3 |
| `purchase` | 5 |

Repeated user-item interactions are aggregated. Ratings are bounded at 5 so repeated activity cannot grow without limit, while stronger actions retain more influence than views.

## Streaming Processing

The Kafka consumer maintains per-user state, updates event-time watermarks, drops events that are outside the allowed lateness period, and checkpoints state for recovery.

Spark Structured Streaming reads the Kafka topic, decodes JSON events, converts event timestamps to Spark timestamps, and computes five-minute sliding windows with a one-minute slide. Windowed counts include views, clicks, searches, add-to-carts, and purchases. Both windowed features and interaction records are written to Parquet with Spark checkpoints.

## Machine Learning

`ml/train.py` loads the interaction Parquet data and creates bounded user-item ratings from the event weights. The evaluation split is built from aggregated user-item pairs:

- Positive recommendation candidates are `click`, `add_to_cart`, and `purchase` pairs.
- The latest eligible positive pair per user is held out for test.
- The previous eligible positive pair is held out for validation when enough training pairs remain.
- Views and searches remain available as training/rating feedback but are not ranking ground truth.
- Held-out users and items must already exist in the ALS training vocabulary.
- A user-item pair is assigned to only one split.

ALS is currently configured with:

- `rank=32`
- `regParam=0.2`
- `maxIter=20`

The model is evaluated on validation RMSE, then refit on training plus validation data before the final test evaluation.

## Evaluation

The pipeline reports:

- RMSE for rating prediction.
- Precision@5.
- Recall@5.
- MAP@5.
- NDCG@5.

For ranking metrics, held-out `click`, `add_to_cart`, and `purchase` pairs are relevant. Recommendations exclude items already present in the user's training interactions. The evaluator fails clearly when no positive test interactions are available instead of silently reporting zero ranking metrics.

## Setup

Install the Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

The project also requires:

- A local Kafka 4.2.1 installation running on `localhost:9092`.
- A Kafka topic named `user-events` with three partitions.
- Java compatible with the installed Spark distribution.
- On Windows, Hadoop `winutils.exe` available through `HADOOP_HOME` for local Spark filesystem operations.
- PySpark 4.2.0 installed in the Python environment used to run `ml/train.py` and `spark/streaming_features.py`.

On Windows, the local environment variables can be set with:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-25"
$env:HADOOP_HOME = "C:\hadoop"
```

The Python producer and consumer use these optional environment variables:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=user-events
KAFKA_GROUP_ID=streamrec-consumer
```

## Running the Project

Run each long-lived component from its own terminal, using the repository paths as working directories.

Start the Kafka producer:

```powershell
python .\producer\main.py
```

Start the Python consumer:

```powershell
python .\consumer\main.py
```

Start Spark Structured Streaming from the `spark` directory:

```powershell
Set-Location .\spark
python .\streaming_features.py
```

After interaction Parquet data has been written, train and evaluate ALS from the `ml` directory:

```powershell
Set-Location .\ml
python .\train.py
```

The ML job reads interaction data from `spark/spark/output/interactions` and prints dataset statistics, model settings, RMSE, and ranking metrics.

