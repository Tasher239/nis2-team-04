import unittest

from check import validate


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.events = [
            {"event_id": 1, "user_id": 101, "amount_cents": 1200},
            {"event_id": 2, "user_id": 202, "amount_cents": 3400},
            {"event_id": 3, "user_id": 101, "amount_cents": 800},
        ]
        self.records = [
            dict(self.events[0], purchase_count=1, total_cents=1200),
            dict(self.events[1], purchase_count=1, total_cents=3400),
            dict(self.events[2], purchase_count=2, total_cents=2000),
        ]

    def test_valid_output_in_different_file_order(self):
        self.assertTrue(validate(self.events, self.records[::-1])["ok"])

    def test_missing_event(self):
        report = validate(self.events, self.records[1:])
        self.assertFalse(report["ok"])
        self.assertEqual(report["missing_event_ids"], [1])

    def test_duplicate_event(self):
        report = validate(self.events, self.records + [self.records[0]])
        self.assertFalse(report["ok"])
        self.assertEqual(report["duplicate_events"], 1)

    def test_wrong_aggregate(self):
        self.records[2]["total_cents"] = 2001
        report = validate(self.events, self.records)
        self.assertFalse(report["ok"])
        self.assertEqual(report["aggregate_errors"], [101])

    def test_wrong_event_contents(self):
        self.records[0]["amount_cents"] = 1201
        report = validate(self.events, self.records)
        self.assertFalse(report["ok"])
        self.assertEqual(report["event_errors"], 1)


if __name__ == "__main__":
    unittest.main()
