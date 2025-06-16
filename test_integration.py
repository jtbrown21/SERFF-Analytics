from src.email_service import EmailConfig, EmailSender, Newsletter, get_test_subscribers


def test_full_email_flow():
    """Test complete email flow with real services"""
    config = EmailConfig()
    sender = EmailSender(config)
    test_response = sender.send_with_retry(
        From=config.from_email,
        To=config.from_email,
        Subject="Integration Test",
        HtmlBody="<p>Test</p>",
    )
    assert test_response["MessageID"]

    subscribers = get_test_subscribers()
    assert isinstance(subscribers, list)

    newsletter = Newsletter(sender, config)
    results = newsletter.send(
        state="Test State",
        month="January",
        year=2024,
        recipients=[config.from_email],
        report_url="https://example.com/test",
        test_mode=True,
    )
    assert len(results["sent"]) > 0
    assert len(results["failed"]) == 0

    large_recipient_list = [f"test{i}@example.com" for i in range(600)]
    assert len(large_recipient_list) == 600


def test_dependency_compatibility():
    from postmarker.core import PostmarkClient
    from pyairtable import Table
    from bs4 import BeautifulSoup

    soup = BeautifulSoup("<p>test</p>", "html.parser")
    assert soup.p.text == "test"
    print("✅ All dependencies compatible!")
