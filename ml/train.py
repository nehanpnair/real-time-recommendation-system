from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from math import log as math_log
from pyspark.sql.functions import (
    collect_list,
    count as spark_count,
    explode,
    least,
    lit,
    max as spark_max,
    monotonically_increasing_id,
    row_number,
    sum as spark_sum,
    struct,
)
from pyspark.sql.window import Window
import os
import platform
from pathlib import Path


if platform.system() == "Windows":
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not Path(java_home, "bin", "java.exe").is_file():
        java_candidates = sorted(Path("C:/Program Files/Java").glob("jdk*/bin/java.exe"))
        if java_candidates:
            os.environ["JAVA_HOME"] = str(java_candidates[-1].parent.parent)

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home or not Path(hadoop_home, "bin", "winutils.exe").is_file():
        raise RuntimeError(
            "Windows Spark requires winutils.exe. Set HADOOP_HOME to a Hadoop "
            "directory containing bin\\winutils.exe before running this script."
        )


spark = (
    SparkSession.builder
    .appName("StreamRec-Recommender")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


EVENT_WEIGHTS = {
    "view": 1.0,
    "click": 2.0,
    "search": 2.0,
    "add_to_cart": 3.0,
    "purchase": 5.0,
}
K = 5


def weighted_event_column():
    weight = lit(0.0)
    for event_type, event_weight in EVENT_WEIGHTS.items():
        weight = when(col("event_type") == event_type, event_weight).otherwise(weight)
    return weight


def aggregate_interactions(events):
    return (
        events.withColumn("event_weight", weighted_event_column())
        .withColumn(
            "is_positive",
            col("event_type").isin("click", "add_to_cart", "purchase").cast("int"),
        )
        .filter(col("event_weight") > 0)
        .groupBy("user_id", "item_id")
        .agg(
            least(lit(5.0), spark_sum("event_weight")).alias("rating"),
            spark_max("event_time").alias("last_event_time"),
            spark_max("is_positive").alias("is_positive"),
        )
    )


def rmse(model, data):
    predictions = model.transform(data).dropna(subset=["prediction"])
    if predictions.limit(1).count() == 0:
        return float("inf")
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    )
    return evaluator.evaluate(predictions)


def ranking_metrics(recommendations, relevant):
    recommendation_rows = recommendations.select("user_id", "recommendations").collect()
    metric_totals = {"precision": 0.0, "recall": 0.0, "map": 0.0, "ndcg": 0.0}
    evaluated_users = 0
    relevant_by_user = {
        row.user_id: set(row.items)
        for row in relevant.groupBy("user_id")
        .agg(collect_list("item_id").alias("items"))
        .collect()
    }

    for row in recommendation_rows:
        relevant_items = relevant_by_user.get(row.user_id, set())
        if not relevant_items:
            continue
        evaluated_users += 1
        hits = 0
        average_precision = 0.0
        dcg = 0.0
        for rank, recommendation in enumerate(row.recommendations[:K], start=1):
            if recommendation.item_id in relevant_items:
                hits += 1
                average_precision += hits / rank
                dcg += 1.0 / (math_log(2.0 + rank) / math_log(2.0))

        relevant_at_k = min(len(relevant_items), K)
        idcg = sum(
            1.0 / (math_log(2.0 + rank) / math_log(2.0))
            for rank in range(1, relevant_at_k + 1)
        )
        metric_totals["precision"] += hits / K
        metric_totals["recall"] += hits / len(relevant_items)
        metric_totals["map"] += average_precision / relevant_at_k
        metric_totals["ndcg"] += dcg / idcg if idcg else 0.0

    if evaluated_users == 0:
        return {name: 0.0 for name in metric_totals}
    return {name: value / evaluated_users for name, value in metric_totals.items()}


try:
    interaction_path = Path("../spark/spark/output/interactions")
    interaction_files = [
        str(path)
        for path in interaction_path.glob("part-*")
        if path.is_file()
    ]
    if not interaction_files:
        raise FileNotFoundError(
            f"No interaction Parquet files found in {interaction_path.resolve()}"
        )

    interactions = (
        spark.read.parquet(*interaction_files)
        .select("user_id", "item_id", "event_type", "event_time")
        .withColumn("event_id", monotonically_increasing_id())
        .dropna(subset=["user_id", "item_id", "event_type", "event_time"])
    )

    total_interactions = interactions.count()
    unique_users = interactions.select("user_id").distinct().count()
    unique_items = interactions.select("item_id").distinct().count()

    pairs = aggregate_interactions(interactions).cache()
    user_pair_counts = pairs.groupBy("user_id").agg(
        spark_count("item_id").alias("user_pair_count")
    )
    item_pair_counts = pairs.groupBy("item_id").agg(
        spark_count("user_id").alias("item_pair_count")
    )

    # Only positive pairs are ranking candidates. At most one pair per item is
    # held out, so every test item remains in the ALS item vocabulary.
    positive_candidates = (
        pairs.filter(col("is_positive") == 1)
        .join(user_pair_counts, "user_id")
        .join(item_pair_counts, "item_id")
        .filter(col("user_pair_count") >= 2)
        .filter(col("item_pair_count") >= 2)
    )
    user_positive_window = Window.partitionBy("user_id").orderBy(
        col("last_event_time").desc(), col("item_id").desc()
    )
    item_candidate_window = Window.partitionBy("item_id").orderBy(
        col("last_event_time").desc(), col("user_id").desc()
    )
    holdout_candidates = (
        positive_candidates
        .withColumn("positive_rank", row_number().over(user_positive_window))
        .filter(col("positive_rank") <= 2)
        .withColumn("item_candidate_rank", row_number().over(item_candidate_window))
        .filter(col("item_candidate_rank") == 1)
        .withColumn(
            "split",
            when(col("positive_rank") == 1, "test")
            .when(
                (col("positive_rank") == 2) & (col("user_pair_count") >= 3),
                "validation",
            ),
        )
        .filter(col("split").isNotNull())
        .select("user_id", "item_id", "split")
    )

    split_pairs = (
        pairs.join(holdout_candidates, ["user_id", "item_id"], "left")
        .withColumn("split", when(col("split").isNull(), "train").otherwise(col("split")))
    )
    train = split_pairs.filter(col("split") == "train").select("user_id", "item_id", "rating").cache()
    validation = split_pairs.filter(col("split") == "validation").select("user_id", "item_id", "rating").cache()
    test = split_pairs.filter(col("split") == "test").select("user_id", "item_id", "rating").cache()
    test_relevant = split_pairs.filter(
        (col("split") == "test") & (col("is_positive") == 1)
    ).select("user_id", "item_id").distinct().cache()

    test_relevant_count = test_relevant.count()
    test_user_count = test_relevant.select("user_id").distinct().count()
    if test_relevant_count == 0:
        raise RuntimeError(
            "No positive test interactions are available. Generate more click, "
            "add_to_cart, or purchase events before evaluating ranking metrics."
        )

    train_users = train.select("user_id").distinct()
    train_items = train.select("item_id").distinct()
    missing_test_users = test.select("user_id").distinct().subtract(train_users).count()
    missing_test_items = test.select("item_id").distinct().subtract(train_items).count()
    if missing_test_users or missing_test_items:
        raise RuntimeError(
            "Invalid split: every test user and test item must exist in training "
            f"(missing users={missing_test_users}, items={missing_test_items})."
        )

    print("\nDataset statistics:")
    print(f"Total interactions: {total_interactions}")
    print(f"Unique users: {unique_users}")
    print(f"Unique items: {unique_items}")
    print(f"Train interactions: {train.count()}")
    print(f"Validation interactions: {validation.count()}")
    print(f"Test interactions: {test.count()}")
    print(f"Test users with relevant positives: {test_user_count}")

    rank, reg_param, max_iter = 32, 0.2, 20
    validation_model = ALS(
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        coldStartStrategy="drop",
        implicitPrefs=False,
    ).fit(train)
    validation_rmse = rmse(validation_model, validation)

    fitting_data = train.unionByName(validation).groupBy("user_id", "item_id").agg(
        least(lit(5.0), spark_sum("rating")).alias("rating")
    )
    model = ALS(
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        coldStartStrategy="drop",
        implicitPrefs=False,
    ).fit(fitting_data)

    test_rmse = rmse(model, test)
    known_items = fitting_data.select("user_id", "item_id").distinct()
    test_users = test.select("user_id").distinct()

    recommendations = (
        model.recommendForAllUsers(500)
        .join(test_users, "user_id")
        .select("user_id", explode("recommendations").alias("recommendation"))
        .select(
            "user_id",
            col("recommendation.item_id").alias("item_id"),
            col("recommendation.rating").alias("score"),
        )
        .join(known_items, ["user_id", "item_id"], "left_anti")
    )
    recommendation_window = Window.partitionBy("user_id").orderBy(col("score").desc())
    recommendations = (
        recommendations.withColumn("rank", row_number().over(recommendation_window))
        .filter(col("rank") <= K)
        .groupBy("user_id")
        .agg(
            collect_list(
                struct("item_id", "score", "rank")
            ).alias("recommendations")
        )
    )

    metrics = ranking_metrics(recommendations, test_relevant)
    print("\nModel: ALS")
    print(f"Rank: {rank}")
    print(f"RegParam: {reg_param}")
    print(f"MaxIter: {max_iter}")
    print(f"Validation RMSE: {validation_rmse:.4f}")
    print(f"RMSE: {test_rmse:.4f}")
    print(f"Precision@5: {metrics['precision']:.4f}")
    print(f"Recall@5: {metrics['recall']:.4f}")
    print(f"MAP@5: {metrics['map']:.4f}")
    print(f"NDCG@5: {metrics['ndcg']:.4f}")
finally:
    spark.stop()