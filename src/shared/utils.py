from datetime import datetime

ALL_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]


def get_current_month_year():
    now = datetime.now()
    return now.strftime("%B"), str(now.year)


REQUIRED_ENV_VARS = [
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
    "POSTMARK_SERVER_TOKEN",
    "GITHUB_USERNAME",
    "GITHUB_REPO_NAME",
    "UNSUBSCRIBE_SECRET",
]


def check_required_env_vars() -> bool:
    """Return True if all required environment variables are set."""
    import os
    import logging

    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        logging.error("Missing required environment variables: %s", ", ".join(missing))
        return False
    return True
