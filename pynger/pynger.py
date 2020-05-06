from argparse import ArgumentParser, FileType
from contextlib import contextmanager
import datetime
import json
import pytz
from pykafka import KafkaClient, SslConfig, Producer
import re
import signal
import socket
import time
from typing import Tuple, Union
import urllib3


class MaxRedirectError(OSError):
    """ Error that signifies that a request has resulted in more redirects than acceptable
    """
    pass


class Metric:
    __slots__ = (
        "pool",
        "tcp_exception",
        "tcp_rt",
        "http_rt",
        "initial_response_code",
        "num_redirects",
        "total_rt",
        "final_response_code",
        "content_found",
        "timestamp"
    )

    def __init__(self):
        # Pool of 1 connection, because we want to measure how long it takes to create one
        # and not to re-use existing
        self.pool = urllib3.PoolManager(maxsize=1)

        # Exception (if any) that is raised during TCP connect
        self.tcp_exception: Union[Exception, None] = None
        # Response time of TCP connection
        self.tcp_rt: float = 0
        # Response time of first HTTP request processed
        self.http_rt: float = 0
        # Response code for the first (and maybe only) request
        self.initial_response_code: Union[int, None] = None
        # Total number of redirects in a request
        self.num_redirects: int = 0
        # Response time of all HTTP requests (with redirects until final response)
        self.total_rt: float = 0
        # Response code for the final response
        self.final_response_code: Union[int, None] = None
        # Shows whether requested content was found
        self.content_found: Union[bool, None] = None
        # The timestamp of the creation of the metric object
        self.timestamp = datetime.datetime.now(tz=pytz.utc)

    @contextmanager
    def connect(self, url: str, http_pool: urllib3.PoolManager):
        response = None
        try:
            # Let the library handle just the HTTP and SSL overhead
            # while we need the details of the communications.
            # This is why we don't want to ignore failures or redirects
            response = http_pool.request(
                'GET',
                url,
                preload_content=False,
                retries=False,
                redirect=False
            )
            yield response
        finally:
            if response:
                response.release_conn()

    def time_connect(self, host: str, port: int, timeout: float = 1):
        sock = socket.socket()
        sock.settimeout(timeout)
        start = time.monotonic_ns()
        try:
            sock.connect((host, port))
            self.tcp_rt = (time.monotonic_ns() - start) * 1.0E-6
        except socket.gaierror:
            self.tcp_exception = socket.gaierror(-2, f"Could not resolve host name: {host}")
        except ConnectionRefusedError:
            self.tcp_exception = ConnectionRefusedError(f"Host refused connection on port {port}")
        except (TimeoutError, socket.timeout):
            self.tcp_exception = TimeoutError(f"Connection timed out after {timeout} seconds")
        finally:
            sock.close()

    def time_http(self, url: str, follow_redirect: bool) -> Tuple[bytes, Union[str, None]]:
        while True:
            if self.num_redirects > 20:
                raise MaxRedirectError("Too many redirects (more than 20)")
            start = time.monotonic_ns()
            response: urllib3.HTTPResponse
            with self.connect(url, self.pool) as response:
                time_delta = (time.monotonic_ns() - start) * 1.0E-6
                self.total_rt += time_delta
                self.final_response_code = response.status
                if not self.initial_response_code:
                    self.http_rt = time_delta
                    self.initial_response_code = response.status
                if follow_redirect and 300 <= response.status < 400:
                    url = response.headers['Location']
                    self.num_redirects += 1
                else:
                    return response.data, response.getheader('content-type', None)

    def keys(self):
        # Implement dict conversion with keys() and __getitem__
        # Inspired by this: https://stackoverflow.com/a/35282286

        # Copy the slots into a list
        result = list(self.__class__.__slots__)
        # We don't need the pool in the end dictionary
        result.remove('pool')
        return result

    def __getitem__(self, key):
        return getattr(self, key)


def parse_args():
    parser = ArgumentParser(description="Produces metrics about website availability")
    parser.add_argument(
        'url',
        help="Target url",
        type=str
    )
    parser.add_argument(
        '-r',
        '--follow-redirect',
        help="Follow HTTP redirects",
        action="store_true"
    )
    parser.add_argument(
        '-s',
        '--search-in-content',
        help="A regular expression to search in the page content",
        type=str
    )
    parser.add_argument(
        '-d',
        '--delay',
        help="Delay between metrics, in seconds (default 60)",
        type=int,
        default=60
    )
    parser.add_argument(
        '--config',
        help='Config file location',
        type=FileType('r', encoding='UTF-8'),
        required=True
    )
    return parser.parse_args()


def get_charset(content_type_str: str) -> Union[str, None]:
    if content_type_str:
        # Convert this "text/html; charset=UTF-8" to this: "UTF-8"
        components = map(lambda s: s.split('='), map(str.strip, content_type_str.split(';')))
        for c in components:
            if c[0].lower() == 'charset':
                return c[1]


def match_content(regex, data: bytes, content_type_str: Union[str, None]) -> bool:
    charset = get_charset(content_type_str) or 'UTF-8'
    try:
        strdata = data.decode(charset)
        return re.search(regex, strdata) is not None
    except ValueError:
        return False


def format_unsupported_types(obj):
    if isinstance(obj, datetime.date) or isinstance(obj, datetime.datetime):
        return datetime.datetime.strftime(obj, "%Y-%m-%d %H:%M:%S%z")
    # Otherwise just str it
    return str(obj)


def connect_kafka(config):
    kafka_config = config["kafka"]
    # https://github.com/Parsely/pykafka
    ssl_config = SslConfig(cafile=kafka_config["cafile"],
                           certfile=kafka_config["certfile"],
                           keyfile=kafka_config["keyfile"])
    client = KafkaClient(hosts=kafka_config["hosts"],
                         ssl_config=ssl_config)
    return client


def produce(producer: Producer, metric: Metric):
    producer.produce(
        json.dumps(dict(metric), default=format_unsupported_types).encode('utf-8')
    )


def work(args):
    config = json.load(args.config)
    url = urllib3.util.parse_url(args.url)
    port = url.port or (443 if url.scheme == 'https' else 80)
    state = {"running": True}

    # Inspired by https://stackoverflow.com/a/31464349
    def proper_exit(signum, frame):
        state["running"] = False

    signal.signal(signal.SIGINT, proper_exit)
    signal.signal(signal.SIGTERM, proper_exit)

    connection = connect_kafka(config["kafka"])
    topic = connection.topics[config["kafka"]["topic"]]
    with topic.get_sync_producer() as producer:
        while True:
            metric = Metric()
            metric.time_connect(host=url.host, port=port)
            if not metric.tcp_exception:
                data, content_type = metric.time_http(url=args.url, follow_redirect=args.follow_redirect)
                if args.search_in_content:
                    metric.content_found = match_content(args.search_in_content, data, content_type)
            produce(producer, metric)
            # Opt for sleeping N * 1 seconds rather than N seconds to be able to have a proper exit
            for i in range(args.delay):
                if not state["running"]:
                    print("Exiting")
                    return
                time.sleep(1)


def main():
    args = parse_args()
    work(args)


if __name__ == '__main__':
    main()
