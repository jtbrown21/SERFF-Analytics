from . import DatabaseManager
from datetime import date
import calendar

__all__ = ["DatabaseManager", "get_month_boundaries", "prepare_template_context"]


def get_month_boundaries(year: int, month: int):
    """Return the first and last day for a given month."""
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)
    return start_date, end_date


def prepare_template_context(airtable_record: dict, year: int, month: int):
    """Create a template context mapping Airtable fields to template variables."""
    start_date, end_date = get_month_boundaries(year, month)
    context = {
        "product_line": airtable_record.get("Product Line", ""),
        "start_date": start_date.strftime("%B %d, %Y"),
        "end_date": end_date.strftime("%B %d, %Y"),
    }
    return context
