import unittest

from metrics import calculate


class MetricsTests(unittest.TestCase):
    def test_restore_and_window_boundary(self):
        records = [{'processed_at_ns': n * 10**9} for n in (1, 2, 6, 7, 8, 16)]
        commands = [
            {'event': 'restore_requested', 'time_ns': 4 * 10**9},
            {'event': 'job_finished', 'time_ns': 17 * 10**9},
        ]
        result = calculate(records, commands)
        self.assertEqual(result['pause_s'], 4)
        self.assertEqual(result['recovery_s'], 2)
        self.assertEqual(result['throughput_events_s'], 0.3)

    def test_short_run_has_no_ten_second_rate(self):
        result = calculate([{'processed_at_ns': 10**9}], [
            {'event': 'job_finished', 'time_ns': 2 * 10**9},
        ])
        self.assertIsNone(result['throughput_events_s'])
        self.assertIsNone(result['recovery_s'])
        self.assertFalse(result['window_complete'])

    def test_missing_events_after_restore(self):
        with self.assertRaises(ValueError):
            calculate([{'processed_at_ns': 10**9}], [
                {'event': 'restore_requested', 'time_ns': 2 * 10**9},
            ])
