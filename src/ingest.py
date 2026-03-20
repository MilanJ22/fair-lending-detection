import io
import requests
import pandas as pd

BASE_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view"

# Five largest U.S. MSAs — rich, diverse lending markets
TARGET_MSAS = {
    "16980": "Chicago-Naperville-Elgin, IL-IN-WI",
    "26420": "Houston-The Woodlands-Sugar Land, TX",
    "12060": "Atlanta-Sandy Springs-Roswell, GA",
    "31080": "Los Angeles-Long Beach-Anaheim, CA",
    "35620": "New York-Newark-Jersey City, NY-NJ-PA",
}

# Columns we care about — drop the 80+ others in the raw LAR
# Note: HMDA uses hyphens in some column names (e.g. derived_msa-md, denial_reason-1)
KEEP_COLS = [
    "lei",
    "activity_year",
    "derived_msa-md",
    "census_tract",
    "action_taken",
    "loan_purpose",
    "loan_type",
    "derived_race",
    "derived_ethnicity",
    "derived_sex",
    "income",
    "loan_amount",
    "denial_reason-1",
    "denial_reason-2",
    "denial_reason-3",
    "denial_reason-4",
]


def fetch_applications(year: int = 2024) -> pd.DataFrame:
    """
    Fetch home purchase loan applications from the CFPB HMDA API.
    Scoped to target MSAs, home purchase loans (purpose=1), and decisive
    action types (originated, approved not accepted, denied).
    """
    params = {
        "years": year,
        "msamds": ",".join(TARGET_MSAS.keys()),
        "action_taken": "1,2,3",  # originated, approved-not-accepted, denied
        "loan_purpose": "1",      # home purchase only
    }

    print(f"Fetching HMDA {year} applications for {len(TARGET_MSAS)} MSAs...")
    print("  This may take 60-120 seconds for a large CSV download...")

    response = requests.get(
        f"{BASE_URL}/csv",
        params=params,
        timeout=300,
        stream=True,
    )
    response.raise_for_status()

    # Stream into memory in 1MB chunks
    content = b""
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        content += chunk

    df = pd.read_csv(io.BytesIO(content), low_memory=False)
    print(f"  Raw records fetched: {len(df):,}")

    # Retain only the columns we need
    available = [c for c in KEEP_COLS if c in df.columns]
    df = df[available].copy()

    # Normalize MSA column name and attach human-readable name
    df = df.rename(columns={"derived_msa-md": "msamd"})
    df["msa_name"] = df["msamd"].astype(str).map(TARGET_MSAS)

    # Normalize denial reason column names (hyphens → underscores)
    df = df.rename(columns={
        "denial_reason-1": "denial_reason_1",
        "denial_reason-2": "denial_reason_2",
        "denial_reason-3": "denial_reason_3",
        "denial_reason-4": "denial_reason_4",
    })

    print(f"  Columns retained: {len(df.columns)}")
    return df


if __name__ == "__main__":
    apps = fetch_applications(2024)
    print(apps["action_taken"].value_counts())
    print(apps["derived_race"].value_counts().head(10))
