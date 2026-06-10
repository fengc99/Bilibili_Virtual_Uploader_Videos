import unittest
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crawler import (
    BilibiliClient,
    EndpointCircuitOpen,
    RiskControlError,
    Store,
    apply_keyword_tag_filter,
    daily_time_windows,
    date_time_windows,
    discover_search_window,
    infer_ai,
    infer_music,
    keyword_matches_description,
    keyword_matches_tags,
    load_keywords,
    local_datetime,
    parse_creators,
    split_time_window,
)


class FakeJSONResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class RuleTests(unittest.TestCase):
    def test_explicit_ai_label(self):
        suspected, score, reasons = infer_ai("普通标题", "", [], "含AI生成内容")
        self.assertEqual(suspected, 1)
        self.assertGreaterEqual(score, 5)
        self.assertTrue(reasons)

    def test_weak_ai_marker_is_not_enough(self):
        suspected, score, _ = infer_ai("【AI又一】又一年蝉鸣", "", [], "")
        self.assertEqual(suspected, 0)
        self.assertEqual(score, 1)

    def test_music_metadata(self):
        candidates, evidence = infer_music("翻唱", "原曲：《归期》\n原唱：钱润玉Runyn", [])
        self.assertIn("《归期》", candidates)
        self.assertIn("钱润玉Runyn", candidates)
        self.assertTrue(evidence)

    def test_joint_submission_order(self):
        creators = parse_creators(
            {
                "owner": {"mid": 9, "name": "owner"},
                "staff": [
                    {"mid": 1, "name": "first", "title": "UP主"},
                    {"mid": 2, "name": "second", "title": "联合投稿"},
                ],
            }
        )
        self.assertEqual([creator["mid"] for creator in creators], [1, 2])

    def test_daily_window_matches_browser_timestamps(self):
        windows = daily_time_windows(date(2026, 4, 1), date(2026, 4, 1), "Europe/Madrid")
        self.assertEqual(windows, [(1774994400, 1775080799)])

    def test_default_windows_use_beijing_time(self):
        windows = daily_time_windows(date(2026, 4, 1), date(2026, 4, 1), "Asia/Shanghai")
        self.assertEqual(windows, [(1774972800, 1775059199)])
        self.assertEqual(local_datetime(1774972800), "2026-04-01T00:00:00+08:00")

    def test_multi_day_initial_windows_cover_range_exactly(self):
        windows = date_time_windows(
            date(2026, 1, 1), date(2026, 1, 10), "Asia/Shanghai", window_days=7
        )
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][1] + 1, windows[1][0])
        self.assertEqual(windows[0][1] - windows[0][0] + 1, 7 * 86400)
        self.assertEqual(windows[1][1] - windows[1][0] + 1, 3 * 86400)

    def test_split_time_window_has_no_gap_or_overlap(self):
        left, right = split_time_window(100, 199)
        self.assertEqual(left, (100, 149))
        self.assertEqual(right, (150, 199))

    def test_keyword_file(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "keywords.txt"
            path.write_text("又一充电中\n# comment\n虚拟主播\n又一充电中\n", encoding="utf-8")
            self.assertEqual(load_keywords("音乐,虚拟主播", path), ["音乐", "虚拟主播", "又一充电中"])

    def test_keyword_tag_match_modes(self):
        tags = ["又一充电中", "虚拟主播切片"]
        self.assertTrue(keyword_matches_tags("又一充电中", tags, "exact"))
        self.assertFalse(keyword_matches_tags("虚拟主播", tags, "exact"))
        self.assertTrue(keyword_matches_tags("虚拟主播", tags, "substring"))

    def test_keyword_description_match(self):
        self.assertTrue(keyword_matches_description("又一 充电中", "这里写了又一充电中录播"))
        self.assertFalse(keyword_matches_description("又一充电中", "其他视频简介"))

    def test_risk_control_response_is_not_retried(self):
        client = BilibiliClient(sleep_seconds=0, retries=4, jitter_seconds=0)
        response = FakeJSONResponse({"code": -352, "message": "风控校验失败"})
        with patch("crawler.urlopen", return_value=response) as request:
            with self.assertRaises(RiskControlError):
                client.get_json("/x/web-interface/view", {"bvid": "BV1234567890"})
        self.assertEqual(request.call_count, 1)
        self.assertEqual(client.request_stats()["risk_control_events"], 1)

    def test_endpoint_circuit_skips_network(self):
        client = BilibiliClient(sleep_seconds=0, jitter_seconds=0)
        client.open_circuit("profile", 60, "test")
        with patch("crawler.urlopen") as request:
            with self.assertRaises(EndpointCircuitOpen):
                client.get_wbi_json("/x/space/wbi/acc/info", {"mid": 1})
        request.assert_not_called()

    def test_cookie_summary_never_exposes_values(self):
        client = BilibiliClient(
            cookie="SESSDATA=secret; bili_jct=csrf; buvid3=device",
            sleep_seconds=0,
        )
        self.assertEqual(
            client.credential_summary(),
            {
                "cookie_present": True,
                "sessdata": True,
                "bili_jct": True,
                "buvid3": True,
                "buvid4": False,
                "dedeuserid": False,
            },
        )

    def test_tag_filter_deletes_video_when_no_keyword_matches(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite3")
            store.add_search_hit("目标词", "BV1234567890", 1, 2, 1, 1, 1)
            store.connection.execute(
                "INSERT INTO videos(bvid, tags_json, fetched_at) VALUES (?, ?, ?)",
                ("BV1234567890", '["其他标签"]', "2026-06-10T00:00:00+00:00"),
            )
            store.commit()
            kept, rejected = apply_keyword_tag_filter(
                store, "BV1234567890", ["其他标签"], "其他简介", "exact"
            )
            self.assertFalse(kept)
            self.assertEqual(rejected, ["目标词"])
            self.assertEqual(
                store.connection.execute("SELECT count(*) FROM videos").fetchone()[0], 0
            )
            self.assertEqual(
                store.connection.execute("SELECT count(*) FROM search_hits").fetchone()[0], 0
            )
            self.assertEqual(
                store.connection.execute(
                    "SELECT count(*) FROM tag_filter_rejections"
                ).fetchone()[0], 1
            )

    def test_tag_filter_keeps_video_if_one_keyword_matches(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite3")
            for keyword in ("保留词", "误命中"):
                store.add_search_hit(keyword, "BV1234567890", 1, 2, 1, 1, 1)
            store.connection.execute(
                "INSERT INTO videos(bvid, tags_json, fetched_at) VALUES (?, ?, ?)",
                ("BV1234567890", '["保留词"]', "2026-06-10T00:00:00+00:00"),
            )
            store.commit()
            kept, rejected = apply_keyword_tag_filter(
                store, "BV1234567890", ["保留词"], "", "exact"
            )
            self.assertTrue(kept)
            self.assertEqual(rejected, ["误命中"])
            self.assertEqual(
                store.connection.execute("SELECT count(*) FROM videos").fetchone()[0], 1
            )
            remaining = store.connection.execute(
                "SELECT keyword FROM search_hits"
            ).fetchone()[0]
            self.assertEqual(remaining, "保留词")

    def test_filter_keeps_video_when_description_matches(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite3")
            store.add_search_hit("目标词", "BV1234567890", 1, 2, 1, 1, 1)
            store.connection.execute(
                "INSERT INTO videos(bvid, tags_json, description, fetched_at) VALUES (?, ?, ?, ?)",
                (
                    "BV1234567890", '["其他标签"]', "简介中包含目标词内容",
                    "2026-06-10T00:00:00+00:00",
                ),
            )
            store.commit()
            kept, rejected = apply_keyword_tag_filter(
                store, "BV1234567890", ["其他标签"], "简介中包含目标词内容", "exact"
            )
            self.assertTrue(kept)
            self.assertEqual(rejected, [])
            self.assertEqual(
                store.connection.execute("SELECT count(*) FROM videos").fetchone()[0], 1
            )

    def test_overflow_window_is_split(self):
        root = (100, 199)

        class FakeClient:
            def get_wbi_json(self, _path, params):
                is_root = (params["pubtime_begin_s"], params["pubtime_end_s"]) == root
                return {
                    "data": {
                        "numResults": 40 if is_root else 0,
                        "numPages": 2 if is_root else 0,
                        "result": [],
                    }
                }

        with TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite3")
            hits, gaps = discover_search_window(
                FakeClient(), store, "test", *root, "Asia/Shanghai",
                max_pages=1, min_window_seconds=60, rediscover=False,
            )
            statuses = [
                row[0] for row in store.connection.execute(
                    "SELECT status FROM search_windows ORDER BY window_begin_ts"
                )
            ]
            self.assertEqual(hits, 0)
            self.assertEqual(gaps, 0)
            self.assertEqual(statuses, ["split", "complete", "complete"])


if __name__ == "__main__":
    unittest.main()
