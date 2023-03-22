import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Config:
    token_url = os.getenv("API_TOKEN_URL")
    base_api_url = os.getenv("BASE_API_URL")
    username = os.getenv("API_USERNAME")
    password = os.getenv("API_PASSWORD")
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_DB')
    POSTGRES_DATABASE_URI = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    filers_endpoint = os.getenv("FILERS_ENDPOINT")
    docket_entries_single_case_endpoint = os.getenv("DOCKET_ENTRIES_SINGLE_ENPOINT")
    docket_entries_multi_case_endpoint = os.getenv("DOCKET_ENTRIES_MULTIPLE_ENPOINT")
    docket_entries_by_date_endpoint = os.getenv("DOCKET_ENTRIES_BY_DATE")
    docket_entries_by_case_and_typeSub = os.getenv("DOCKET_ENTRIES_BY_CASE_AND_SUBTYPE")
    docket_entries_by_case_and_type = os.getenv("DOCKET_ENTRIES_BY_CASE_AND_TYPE")
    civil_cases_endpoint = os.getenv("CIVIL_CASES_ENDPOINT")
    pending_cases_endpoint = os.getenv("PENDING_CASES_ENDPOINT")
    pending_cases_endpoint_by_judge = os.getenv("PENDING_CASES_ENDPOINT_BY_JUDGE")
    time_in_court_endpoint = os.getenv("TIME_IN_COURT")
