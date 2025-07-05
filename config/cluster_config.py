cluster_config = {
    "cluster_name": "credit-card-etl-cluster",
    "spark_version": "11.3.x-scala2.12",  # Always use the latest stable
    "node_type_id": "i3.xlarge",  # Memory-optimized for our workload
    "autoscale": {
        "min_workers": 2,  # Don't go to zero - startup time kills you
        "max_workers": 20  # Set a reasonable limit
    },
    "auto_termination_minutes": 30,  # Save money!
    "enable_elastic_disk": True,  # Because you'll run out of space
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",  # Let Spark be smart
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",  # Fewer small files
        "spark.databricks.delta.autoCompact.enabled": "true"
    }
}
