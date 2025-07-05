import boto3
def publish_pipeline_metrics(stage, duration, record_count, error_count):
    """
    The metrics that matter when things go sideways
    """
    cloudwatch = boto3.client('cloudwatch')

    # These are the metrics I actually look at
    metrics = [
        {
            'MetricName': 'ProcessingDuration',
            'Value': duration,
            'Unit': 'Seconds',
            'Dimensions': [{'Name': 'Stage', 'Value': stage}]
        },
        {
            'MetricName': 'RecordsProcessed',
            'Value': record_count,
            'Unit': 'Count',
            'Dimensions': [{'Name': 'Stage', 'Value': stage}]
        },
        {
            'MetricName': 'ErrorCount',
            'Value': error_count,
            'Unit': 'Count',
            'Dimensions': [{'Name': 'Stage', 'Value': stage}]
        }
    ]

    cloudwatch.put_metric_data(
        Namespace='CreditCard/ETL',  # Namespace everything!
        MetricData=metrics
    )