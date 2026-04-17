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

def create_processed_events_sink_postgres(t_env):
    table_name = 'healthcare_events'
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
            alert_flag BOOLEAN
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
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
    postgres_sink = create_processed_events_sink_postgres(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {postgres_sink}
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
            alert_flag
        FROM {source_table}
        """
    ).wait()

if __name__ == '__main__':
    main()