import os
import unittest
from unittest.mock import patch

import pandas as pd
from slack_bolt import App as SlackBoltApp

os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test-token")
os.environ.setdefault("SLACK_SIGNING_SECRET", "test-signing-secret")
os.environ["DATABASE_URL"] = ""


class TestSlackBoltApp(SlackBoltApp):
    def __init__(self, *args, **kwargs):
        kwargs["token_verification_enabled"] = False
        super().__init__(*args, **kwargs)


with patch("slack_bolt.App", TestSlackBoltApp):
    import app as dealer_app


class FindClosestTests(unittest.TestCase):
    def setUp(self):
        self.original_df = dealer_app.df
        self.original_safe_geocode = dealer_app.safe_geocode
        dealer_app.app.config["TESTING"] = True
        dealer_app.df = pd.DataFrame(
            [
                {
                    "Name": "OTOVO closest location",
                    "Phone": "(833) 317-6937",
                    "Latitude": 0.1,
                    "Longitude": 0,
                    "Notes": "",
                },
                {
                    "Name": "OTOVO farther location",
                    "Phone": "833-317-6937",
                    "Latitude": 0.2,
                    "Longitude": 0,
                    "Notes": "",
                },
                {
                    "Name": "Independent dealer",
                    "Phone": "555-0100",
                    "Latitude": 0.3,
                    "Longitude": 0,
                    "Notes": "",
                },
                {
                    "Name": "Dealer without phone one",
                    "Phone": "",
                    "Latitude": 0.4,
                    "Longitude": 0,
                    "Notes": "",
                },
                {
                    "Name": "Dealer without phone two",
                    "Phone": "",
                    "Latitude": 0.5,
                    "Longitude": 0,
                    "Notes": "",
                },
            ]
        )
        dealer_app.safe_geocode = lambda address: {"lat": 0, "lon": 0}

    def tearDown(self):
        dealer_app.df = self.original_df
        dealer_app.safe_geocode = self.original_safe_geocode

    def test_find_closest_returns_only_nearest_location_per_phone(self):
        response = dealer_app.app.test_client().post(
            "/find-closest", json={"address": "Test address"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [dealer["name"] for dealer in response.get_json()],
            [
                "OTOVO closest location",
                "Independent dealer",
                "Dealer without phone one",
                "Dealer without phone two",
            ],
        )


if __name__ == "__main__":
    unittest.main()
