from __future__ import annotations

import unittest

from collector.kbo_hitter_parser import _map_hitter_columns


class KboHitterParserTest(unittest.TestCase):
    def test_map_hitter_columns_keeps_run_distinct_from_rbi(self):
        headers = ["선수명", "타수", "득점", "타점", "안타"]

        mapped = _map_hitter_columns(headers)

        self.assertEqual(mapped["R"], "득점")
        self.assertEqual(mapped["RBI"], "타점")

    def test_map_hitter_columns_does_not_match_single_letter_alias_fuzzily(self):
        headers = ["선수명", "타수", "RBI", "안타"]

        mapped = _map_hitter_columns(headers)

        self.assertIsNone(mapped["R"])
        self.assertEqual(mapped["RBI"], "RBI")


if __name__ == "__main__":
    unittest.main()
