import pytest
from serff_analytics.ingest.airtable_sync import AirtableSync

sync = AirtableSync.__new__(AirtableSync)

def test_percentage_string_parses_correctly():
    assert sync._parse_number("7.00%") == pytest.approx(0.07)

def test_regular_numeric_string():
    assert sync._parse_number("1234") == 1234.0
