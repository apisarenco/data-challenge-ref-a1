from io import StringIO
from pynger import pynger
import unittest
from types import SimpleNamespace


class MyTestCase(unittest.TestCase):
    def test_enrich_args(self):
        config = StringIO("""{"operation": {"foo": "bar"}}""")
        # noinspection PyTypeChecker
        args = pynger.enrich_args(SimpleNamespace(
            config=config,
            follow_redirect=True
        ))
        assert hasattr(args, "config")
        assert "operation" in args.config
        assert "foo" in args.config["operation"]
        assert args.config["operation"]["foo"] == "bar"

        assert hasattr(args, "delay")
        assert args.delay == 60

        assert hasattr(args, "follow_redirect")
        assert args.follow_redirect == True

        assert hasattr(args, "foo")
        assert args.foo == "bar"

    def test_get_charset(self):
        assert pynger.get_charset("test/html; charset=UTF-8") == 'UTF-8'
        assert pynger.get_charset(";charset=ISO-8859-1") == 'ISO-8859-1'
        assert pynger.get_charset("charset=ISO-8859-1") == 'ISO-8859-1'
        assert pynger.get_charset("CHARSET = ISO-8859-1") == 'ISO-8859-1'

    def test_match_content(self):
        assert pynger.match_content('abc', b"babcebcabc", 'charset=UTF-8')
        assert pynger.match_content('a[b]?c', b"bacebcaby", 'charset=UTF-8')
        assert not pynger.match_content('a[b]?c', b"baebcabz", 'charset=UTF-8')

    def test_time_connect(self):
        metric = pynger.Metric()
        # test with CloudFlare DNS
        metric.time_connect('1.1.1.1', 53)
        assert metric.tcp_rt > 0
        assert metric.tcp_exception is None

        metric = pynger.Metric()
        # test with bogus host
        metric.time_connect('foo.bar.bzzzazzz23', 12345)
        assert metric.tcp_rt == 0
        assert metric.tcp_exception is not None

    def test_time_http(self):
        # test with redirect-awareness
        metric = pynger.Metric()
        data, content_type = metric.time_http("http://github.com", follow_redirect=True)
        assert data is not None
        assert content_type is not None
        assert metric.total_rt > 0
        assert metric.http_rt > 0
        assert metric.final_response_code >= 400 or metric.final_response_code < 300
        # It should redirect to https
        assert 300 <= metric.initial_response_code < 400
        assert metric.num_redirects > 0

        # test without redirect-awareness
        metric = pynger.Metric()
        data, content_type = metric.time_http("http://github.com", follow_redirect=False)
        assert data is not None
        assert content_type is None
        assert metric.total_rt > 0
        assert metric.http_rt > 0
        # No redirects followed, both response codes are the same
        assert 300 <= metric.initial_response_code < 400
        assert metric.final_response_code == metric.initial_response_code
        assert metric.num_redirects == 0


    def test_metric_dict(self):
        metric = pynger.Metric()
        asdict = dict(metric)
        assert set(asdict.keys()) == {
            "tcp_exception",
            "tcp_rt",
            "http_rt",
            "initial_response_code",
            "num_redirects",
            "total_rt",
            "final_response_code",
            "content_found",
            "timestamp"
        }


if __name__ == '__main__':
    unittest.main()
