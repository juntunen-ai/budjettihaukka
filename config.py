import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


BASE_DIR = Path(__file__).resolve().parent
_load_env_file(BASE_DIR / ".env.local")
_load_env_file(BASE_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    project_id: str = os.getenv("BUDJETTIHAUKKA_PROJECT_ID", "budjettihaukka-gpt")
    runtime_project_id: str = os.getenv("BUDJETTIHAUKKA_RUNTIME_PROJECT_ID", "")
    location: str = os.getenv("BUDJETTIHAUKKA_LOCATION", "us-central1")
    data_source: str = os.getenv("BUDJETTIHAUKKA_DATA_SOURCE", "bigquery").lower()
    dataset: str = os.getenv("BUDJETTIHAUKKA_DATASET", "valtiodata")
    # App queries the promoted semantic layer alias; raw ingest table stays
    # available for pipeline scripts via raw_table.
    table: str = os.getenv("BUDJETTIHAUKKA_TABLE", "valtiontalous_semantic_current")
    raw_table: str = os.getenv("BUDJETTIHAUKKA_RAW_TABLE", "valtiontalous_raw")
    demo_sql_table: str = os.getenv("BUDJETTIHAUKKA_DEMO_SQL_TABLE", "budjettidata_demo")
    demo_sheet_id_2022: str = os.getenv("BUDJETTIHAUKKA_DEMO_SHEET_ID_2022", "")
    demo_sheet_id_2023: str = os.getenv("BUDJETTIHAUKKA_DEMO_SHEET_ID_2023", "")
    demo_sheet_id_2024: str = os.getenv("BUDJETTIHAUKKA_DEMO_SHEET_ID_2024", "")
    gemini_model: str = os.getenv("BUDJETTIHAUKKA_GEMINI_MODEL", "gemini-2.5-pro-preview-03-25")
    enable_llm_query_plan: bool = _env_bool("BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN", False)
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    google_application_credentials: str | None = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    max_query_bytes: int = _env_int("BUDJETTIHAUKKA_MAX_QUERY_BYTES", 1_000_000_000)
    sql_max_limit: int = _env_int("BUDJETTIHAUKKA_SQL_MAX_LIMIT", 1000)
    bq_auto_repair_attempts: int = _env_int("BUDJETTIHAUKKA_BQ_AUTO_REPAIR_ATTEMPTS", 2)
    clarification_required_confidence: float = _env_float(
        "BUDJETTIHAUKKA_CLARIFICATION_REQUIRED_CONFIDENCE",
        0.75,
    )
    observability_log_path: str = os.getenv(
        "BUDJETTIHAUKKA_OBSERVABILITY_LOG_PATH",
        "agent_data/query_observability.jsonl",
    )
    question_library_log_path: str = os.getenv(
        "BUDJETTIHAUKKA_QUESTION_LIBRARY_LOG_PATH",
        "agent_data/question_library.jsonl",
    )
    question_library_backend: str = os.getenv(
        "BUDJETTIHAUKKA_QUESTION_LIBRARY_BACKEND",
        "auto",
    ).lower()
    firestore_database: str = os.getenv("BUDJETTIHAUKKA_FIRESTORE_DATABASE", "(default)")
    firestore_question_collection: str = os.getenv(
        "BUDJETTIHAUKKA_FIRESTORE_QUESTION_COLLECTION",
        "question_library",
    )
    require_auth: bool = _env_bool("BUDJETTIHAUKKA_REQUIRE_AUTH", False)
    firebase_auth_project_id: str = os.getenv("BUDJETTIHAUKKA_FIREBASE_AUTH_PROJECT_ID", "")
    admin_api_key: str = os.getenv("BUDJETTIHAUKKA_ADMIN_KEY", "")
    free_queries_per_session: int = _env_int("BUDJETTIHAUKKA_FREE_QUERIES_PER_SESSION", 25)
    show_ads: bool = _env_bool("BUDJETTIHAUKKA_SHOW_ADS", True)
    adsense_client_id: str = os.getenv("BUDJETTIHAUKKA_ADSENSE_CLIENT_ID", "")
    adsense_slot_top: str = os.getenv("BUDJETTIHAUKKA_ADSENSE_SLOT_TOP", "")
    adsense_slot_bottom: str = os.getenv("BUDJETTIHAUKKA_ADSENSE_SLOT_BOTTOM", "")
    ad_placeholder_text: str = os.getenv("BUDJETTIHAUKKA_AD_PLACEHOLDER_TEXT", "Mainospaikka")
    analytics_api_url: str = os.getenv("BUDJETTIHAUKKA_ANALYTICS_API_URL", "http://127.0.0.1:8000")
    cors_origins_raw: str = os.getenv(
        "BUDJETTIHAUKKA_CORS_ORIGINS",
        "http://localhost:8501,http://127.0.0.1:8501",
    )
    use_backend_api: bool = _env_bool("BUDJETTIHAUKKA_USE_BACKEND_API", False)
    ontology_path: str = os.getenv(
        "BUDJETTIHAUKKA_ONTOLOGY_PATH",
        "data/ontology/budjettihaukka_ontology.yaml",
    )
    ontology_table_prefix: str = os.getenv("BUDJETTIHAUKKA_ONTOLOGY_TABLE_PREFIX", "ontology")

    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins_raw.split(",") if origin.strip()]

    @property
    def llm_provider(self) -> str:
        return "aistudio" if self.gemini_api_key else "vertex"

    @property
    def use_google_sheets_demo(self) -> bool:
        return self.data_source == "google_sheets"

    @property
    def demo_sheet_ids(self) -> dict[str, str]:
        return {
            "2022": self.demo_sheet_id_2022,
            "2023": self.demo_sheet_id_2023,
            "2024": self.demo_sheet_id_2024,
        }

    @property
    def has_adsense(self) -> bool:
        return bool(self.adsense_client_id)


settings = Settings()
