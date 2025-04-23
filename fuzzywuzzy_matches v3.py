import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm

# Load the datasets
scrape_spy_midnight = pd.read_csv(r"C:\Users\adamp\OneDrive\Desktop\_giving_realty_\scrape_spy_midnight.csv")
court_calendar_records = pd.read_csv(r"C:\Users\adamp\OneDrive\Desktop\_giving_realty_\court_calendar_records.csv")

# Preprocess: Remove entries with 'LLC' and clean names
scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("LLC", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("CONSTRUCTION", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("PROPERTIES", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("BUILDERS", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("ENTERPRISES", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("HOMES", na=False)]

scrape_spy_midnight = scrape_spy_midnight[~scrape_spy_midnight["grantee"].str.contains("PARTNERSHIP", na=False)]

court_calendar_records = court_calendar_records[~court_calendar_records["Name"].str.contains("LLC", na=False)]

# Define function to split names from `Name` column in court_calendar_records
def split_names(name):
    if pd.isnull(name):
        return None, None
    parts = name.split(",")
    last_name = parts[0].strip().lower()
    first_name = parts[1].strip().lower() if len(parts) > 1 else None
    return last_name, first_name

# Define function to split grantee names from `grantee` column in scrape_spy_midnight
def split_grantee(grantee):
    if pd.isnull(grantee):
        return None, None
    parts = grantee.split("&")
    last_name = parts[0].strip().lower()
    first_name = parts[1].strip().lower() if len(parts) > 1 else None
    return last_name, first_name

# Apply split functions
scrape_spy_midnight[["Grantee_Last", "Grantee_First"]] = scrape_spy_midnight["grantee"].apply(
    lambda x: pd.Series(split_grantee(x))
)
court_calendar_records[["Name_Last", "Name_First"]] = court_calendar_records["Name"].apply(
    lambda x: pd.Series(split_names(x))
)

# Define function to perform fuzzy matching
def match_names(last_name, first_name, names_df, threshold=95):
    matches = []
    for idx, row in names_df.iterrows():
        # Calculate fuzzy scores for last and first names
        last_name_score = fuzz.partial_ratio(last_name, row["Name_Last"]) if last_name else 0
        first_name_score = fuzz.partial_ratio(first_name, row["Name_First"]) if first_name else 0

        # Combine scores with weights
        score = (0.6 * last_name_score) + (0.4 * first_name_score)

        # Check if the score meets the threshold
        if score >= threshold:
            matches.append({
                "Grantee_Last": last_name,
                "Grantee_First": first_name,
                "Name_Last": row["Name_Last"],
                "Name_First": row["Name_First"],
                "Score": score,
                "Court_Record_Index": idx,
                "Spy_Record_Index": row.name,  # Add the index from scrape_spy_midnight
            })
    return matches

# Perform fuzzy matching
results = []
for idx, row in tqdm(scrape_spy_midnight.iterrows(), total=len(scrape_spy_midnight), desc="Processing Records"):
    last_name = row["Grantee_Last"]
    first_name = row["Grantee_First"]

    # Match against court calendar records
    matches = match_names(last_name, first_name, court_calendar_records, threshold=95)
    for match in matches:
        match["Spy_Record_Index"] = idx  # Add scrape_spy_midnight index
        results.append(match)
        # Print matched records dynamically
        print(f"Match found: {match['Grantee_Last']} {match['Grantee_First']} matched with "
              f"{match['Name_Last']} {match['Name_First']} with score {match['Score']}")

# Convert results to DataFrame
matched_df = pd.DataFrame(results)

# Add context from original datasets
matched_df = matched_df.merge(
    scrape_spy_midnight,
    left_on="Spy_Record_Index",
    right_index=True,
    how="left"
).merge(
    court_calendar_records,
    left_on="Court_Record_Index",
    right_index=True,
    how="left",
    suffixes=("_Spy", "_Court")
)

# Save the results to a CSV
matched_df.to_csv(r"C:\Users\adamp\OneDrive\Desktop\_giving_realty_\matches.csv", index=False)

# Print summary
print(f"Total matches found: {len(matched_df)}")
print(matched_df.head())
