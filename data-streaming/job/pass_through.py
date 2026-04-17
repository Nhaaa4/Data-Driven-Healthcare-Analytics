import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            subject_id INTEGER,
            device_id INTEGER,
            event_time BIGINT,
            heart_rate INT,
            oxygen_level INT,
            systolic_bp INT,
            diastolic_bp INT,
            body_temperature FLOAT,
            activity_level VARCHAR(255),
            alert_flag BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'healthcare_vitals',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_healthcare_events_sink_gcs(t_env):
    table_name = "healthcare_events_gcs"
    bucket_name = os.environ.get("GCS_DATA_LAKE_BUCKET", "").strip()

    if not bucket_name:
        raise ValueError("GCS_DATA_LAKE_BUCKET environment variable is required")

    sink_path = f"gs://{bucket_name}/healthcare_vitals"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            subject_id INTEGER,
            device_id INTEGER,
            event_time TIMESTAMP,
            heart_rate INT,
            oxygen_level INT,
            systolic_bp INT,
            diastolic_bp INT,
            body_temperature FLOAT,
            activity_level VARCHAR(255),
            alert_flag BOOLEAN,
            event_date STRING
        ) PARTITIONED BY (event_date)
        WITH (
            'connector' = 'filesystem',
            'path' = '{sink_path}',
            'format' = 'parquet',
            
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '1 min',
            'sink.partition-commit.policy.kind' = 'success-file',
            
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '30 min',
            'sink.rolling-policy.check-interval' = '1 min'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    gcs_sink = create_healthcare_events_sink_gcs(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {gcs_sink}
        SELECT
            subject_id,
            device_id,
            TO_TIMESTAMP_LTZ(event_time, 3) as event_time,
            heart_rate,
            oxygen_level,
            systolic_bp,
            diastolic_bp,
            body_temperature,
            activity_level,
            alert_flag,
            DATE_FORMAT(TO_TIMESTAMP_LTZ(event_time, 3), 'yyyy-MM-dd') as event_date
        FROM {source_table}
        """
    ).wait()

if __name__ == '__main__':
    main()
