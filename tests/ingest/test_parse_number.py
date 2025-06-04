from serff_analytics.ingest.airtable_sync import AirtableSync

sync = AirtableSync.__new__(AirtableSync)

def test_percentage_string_returns_none():
    assert sync._parse_number("7.00%") is None

def test_regular_numeric_string():
    assert sync._parse_number("1234") == 1234.0
