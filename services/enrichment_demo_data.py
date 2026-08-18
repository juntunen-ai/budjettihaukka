from __future__ import annotations

from pathlib import Path

import pandas as pd


REFERENCE_DIR = Path(__file__).resolve().parents[1] / "data" / "reference"

DEFLATOR_LABELS = {
    "cpi_general_purchasing_power": "Kuluttajahinnat",
    "public_service_cost_municipal": "Julkisten palvelujen kustannukset",
    "building_cost_investment": "Rakentamisen kustannukset",
}

FINAL_ACCOUNT_LABELS = {
    "balance_sheet_government_debt_eur": "Valtionvelka",
    "balance_sheet_total_eur": "Taseen loppusumma",
    "transfer_expenses_eur": "Siirtomenot",
    "tax_revenue_eur": "Verotulot",
    "net_borrowing_eur": "Nettolainanotto",
    "income_statement_deficit_eur": "Tilikauden alijäämä",
}


def load_reference_data() -> dict[str, pd.DataFrame]:
    return {
        "deflators": pd.read_csv(REFERENCE_DIR / "official_deflator_reference_v1.csv"),
        "sectors": pd.read_csv(REFERENCE_DIR / "official_sector_indicator_v1.csv"),
        "grants": pd.read_csv(REFERENCE_DIR / "official_grants_okm_pilot_v1.csv"),
        "final_accounts": pd.read_csv(REFERENCE_DIR / "official_final_accounts_reference_v1.csv"),
    }


def purchasing_power_series(
    deflators: pd.DataFrame,
    nominal_amount_eur: float,
    deflator_ids: list[str],
    reference_year: int = 2024,
) -> pd.DataFrame:
    selected = deflators[deflators["deflator_id"].isin(deflator_ids)].copy()
    reference = (
        selected[selected["year"].eq(reference_year)]
        .set_index("deflator_id")["index_value"]
        .to_dict()
    )
    selected = selected[selected["deflator_id"].isin(reference)].copy()
    selected["real_amount_eur"] = selected.apply(
        lambda row: nominal_amount_eur * reference[row["deflator_id"]] / row["index_value"],
        axis=1,
    )
    selected["real_amount_million_eur"] = selected["real_amount_eur"] / 1_000_000
    selected["deflator_label"] = selected["deflator_id"].map(DEFLATOR_LABELS)
    return selected.sort_values(["deflator_label", "year"]).reset_index(drop=True)


def education_series(sectors: pd.DataFrame, regions: list[str]) -> pd.DataFrame:
    selected = sectors[
        sectors["dashboard_id"].eq("education")
        & sectors["region_name_fi"].isin(regions)
    ].copy()
    graduates = selected[selected["metric_id"].eq("education_graduates_previous_year")].copy()
    baseline = graduates.groupby("region_name_fi")["value"].transform("first")
    graduates["display_value"] = graduates["value"] / baseline * 100
    graduates["display_metric"] = "Tutkinnon suorittaneet (2015=100)"

    employment = selected[selected["metric_id"].eq("education_employed_one_year_pct")].copy()
    employment["display_value"] = employment["value"]
    employment["display_metric"] = "Työlliset vuoden kuluttua (%)"
    return pd.concat([graduates, employment], ignore_index=True)


def health_scatter(sectors: pd.DataFrame, year: int) -> pd.DataFrame:
    health = sectors[
        sectors["dashboard_id"].eq("health") & sectors["year"].eq(year)
    ].copy()
    wide = health.pivot_table(
        index=["year", "region_code", "region_name_fi", "region_type"],
        columns="metric_id",
        values="value",
    ).reset_index()
    return wide.dropna(
        subset=[
            "primary_care_doctor_contacts_per_1000",
            "self_rated_health_mediocre_or_worse_pct",
        ]
    )


def grant_funnels(grants: pd.DataFrame) -> pd.DataFrame:
    values = grants.set_index("metric_id")["value"]
    rows = [
        {
            "measure": "Hakemukset",
            "stage": "Haettu",
            "stage_order": 1,
            "value": values["applications_received_count"],
            "display_value": f"{int(values['applications_received_count']):,}".replace(",", " "),
            "share": 1.0,
        },
        {
            "measure": "Hakemukset",
            "stage": "Myönteinen päätös",
            "stage_order": 2,
            "value": values["positive_decisions_count"],
            "display_value": f"{int(values['positive_decisions_count']):,}".replace(",", " "),
            "share": values["positive_decisions_count"] / values["applications_received_count"],
        },
        {
            "measure": "Eurot",
            "stage": "Haettu",
            "stage_order": 1,
            "value": values["applied_amount_eur"],
            "display_value": f"{values['applied_amount_eur'] / 1_000_000:.1f} M€",
            "share": 1.0,
        },
        {
            "measure": "Eurot",
            "stage": "Myönnetty",
            "stage_order": 2,
            "value": values["granted_amount_eur"],
            "display_value": f"{values['granted_amount_eur'] / 1_000_000:.1f} M€",
            "share": values["granted_amount_eur"] / values["applied_amount_eur"],
        },
    ]
    return pd.DataFrame(rows)


def final_account_scale(final_accounts: pd.DataFrame) -> pd.DataFrame:
    selected = final_accounts[
        final_accounts["metric_id"].isin(FINAL_ACCOUNT_LABELS)
    ].copy()
    selected["metric_label"] = selected["metric_id"].map(FINAL_ACCOUNT_LABELS)
    selected["value_billion_eur"] = selected["official_value"] / 1_000_000_000
    return selected.sort_values("value_billion_eur", ascending=False).reset_index(drop=True)
