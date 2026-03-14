import os

API_BASE_URL = "http://localhost:5050"


BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LANDING_DIR = os.path.join(PROJECT_ROOT, "output", "landing", "inbound")
BRONZE_DIR = os.path.join(PROJECT_ROOT, "output", BRONZE)
SILVER_DIR = os.path.join(PROJECT_ROOT, "output", SILVER)
GOLD_DIR = os.path.join(PROJECT_ROOT, "output", GOLD)
