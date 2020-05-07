from argparse import ArgumentParser, FileType, Namespace
import contextlib
import json
import psycopg2
from pykafka import KafkaClient, SslConfig, SimpleConsumer
import signal
from typing import Dict, Union


@contextlib.contextmanager
def connect(config: Dict[str, Union[str, int]]):
    conn, cursor = None, None
    try:
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        yield conn, cursor
    finally:
        if conn:
            conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def connect_kafka(config: Dict[str, Union[str, int]]) -> KafkaClient:
    # https://github.com/Parsely/pykafka
    ssl_config = SslConfig(cafile=config["cafile"],
                           certfile=config["certfile"],
                           keyfile=config["keyfile"])
    client = KafkaClient(hosts=config["hosts"],
                         ssl_config=ssl_config)
    return client


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Uploads metrics from Kafka to Postgresql")
    parser.add_argument(
        'config',
        help='Config file location',
        type=FileType('r', encoding='UTF-8')
    )
    args = parser.parse_args()
    args.config = json.load(args.config)
    return args


def ensure_table(pg_config):
    with connect(pg_config) as (conn, curs):
        curs.execute("""
        CREATE TABLE IF NOT EXISTS metric (
            created_at TIMESTAMPTZ PRIMARY KEY,
            tcp_exception TEXT,
            tcp_rt FLOAT,
            http_rt FLOAT,
            initial_rc SMALLINT,
            num_redirects SMALLINT,
            total_rt FLOAT,
            final_rc SMALLINT,
            content_found BOOLEAN
        );
        """)


def start_postgresql_writer(consumer: SimpleConsumer, pg_config: Dict[str, Union[str, int]]):
    with connect(pg_config) as (conn, curs):
        curs.execute("""
            PREPARE insert_metric AS
            INSERT INTO metric
            (
                created_at,
                tcp_exception,
                tcp_rt,
                http_rt,
                initial_rc,
                num_redirects,
                total_rt,
                final_rc,
                content_found
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (created_at) DO NOTHING;
        """)
        for message in consumer:
            if message is not None:
                metric = json.loads(message.value.decode('utf-8'))
                curs.execute("""
                    EXECUTE insert_metric (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    metric["timestamp"],
                    metric["tcp_exception"],
                    metric["tcp_rt"],
                    metric["http_rt"],
                    metric["initial_response_code"],
                    metric["num_redirects"],
                    metric["total_rt"],
                    metric["final_response_code"],
                    metric["content_found"]
                ))
                # Commit once every 16 messages
                if message.offset & 0xF == 0xF:
                    conn.commit()


def work(args):
    # Inspired by https://stackoverflow.com/a/31464349
    def proper_exit(signum, frame):
        exit()

    signal.signal(signal.SIGINT, proper_exit)
    signal.signal(signal.SIGTERM, proper_exit)

    ensure_table(args.config["postgresql"])

    kafka_conn = connect_kafka(args.config["kafka"])
    topic = kafka_conn.topics[args.config["kafka"]["topic"]]
    consumer = topic.get_simple_consumer()
    start_postgresql_writer(consumer, args.config["postgresql"])


def main():
    args = parse_args()
    work(args)


if __name__ == '__main__':
    main()
