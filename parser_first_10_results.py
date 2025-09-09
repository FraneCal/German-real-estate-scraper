import os
import re
import pandas as pd
from bs4 import BeautifulSoup

HTML_DIR = "htmls"
EXCEL_FILE = "results.xlsx"
OUTPUT_FILE = "results_with_first_10.xlsx"  # adjust as needed

def safe_text(tag):
    """Helper to extract text or return '-' if missing."""
    return tag.get_text(" ", strip=True) if tag else "-"

def clean_text_for_excel(text):
    """Remove illegal characters that cannot be written to Excel."""
    if text == "-" or pd.isna(text):
        return "-"
    # Remove control characters except newline and tab
    return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', str(text))

def parse_html(file_path):
    """Parse one HTML file and extract fields."""
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # --- Title ---
    title_tag = soup.find("h1", id="title")
    title = safe_text(title_tag)

    # --- Stars ---
    stars_tag = soup.find("span", class_="text-green_dark")
    stars = safe_text(stars_tag)

    # --- Reviews ---
    reviews_tag = soup.find("span", class_="text-mid_grey font-normal whitespace-nowrap")
    reviews = safe_text(reviews_tag)
    reviews = reviews.replace("(", "").replace(")", "").strip()  # remove parentheses

    # --- Bedrooms ---
    bedrooms_tag = soup.find("figcaption", string=lambda t: t and "Schlafzimmer" in t)
    bedrooms = safe_text(bedrooms_tag)

    # --- Size in m² ---
    size_tag = soup.find("figcaption", string=lambda t: t and ("m²" in t or "m&sup2;" in t))
    size = safe_text(size_tag)

    # --- Max persons ---
    max_tag = soup.find("figcaption", string=lambda t: t and "Personen" in t)
    max_people = safe_text(max_tag)

    # --- Additional features ---
    features = []
    for tag in soup.find_all("figcaption"):
        txt = tag.get_text(strip=True)
        if not txt or txt == "-":
            continue
        if any(txt == val for val in [bedrooms, size, max_people] if val != "-"):
            continue
        if "Haftung" in txt or "Unterkunft" in txt:
            continue
        if "text-sm text-mid_grey" in tag.get("class", []):
            continue
        features.append(txt)
    features_text = "; ".join(features) if features else "-"

    # --- Lat/Lon from <figure id="map"> ---
    lat, lon = "-", "-"
    fig_tag = soup.find("figure", id="map")
    if fig_tag:
        lat = fig_tag.get("data-lat", "-")
        lon = fig_tag.get("data-lon", "-")

    # --- Description ---
    desc_container = soup.find("div", id="manualBlock")
    description = safe_text(desc_container)

    # Clean all text for Excel
    title = clean_text_for_excel(title)
    stars = clean_text_for_excel(stars)
    reviews = clean_text_for_excel(reviews)
    bedrooms = clean_text_for_excel(bedrooms)
    size = clean_text_for_excel(size)
    max_people = clean_text_for_excel(max_people)
    features_text = clean_text_for_excel(features_text)
    lat = clean_text_for_excel(lat)
    lon = clean_text_for_excel(lon)
    description = clean_text_for_excel(description)

    return title, stars, reviews, bedrooms, size, max_people, features_text, lat, lon, description

def main():
    # Load existing Excel
    df = pd.read_excel(EXCEL_FILE)

    # Add new columns if not already there
    new_cols = ["Title", "Stars", "No. of reviews", "No. of bedrooms",
                "Size [m2]", "Max people", "Features", "Latitude", "Longitude", "Description"]
    for col in new_cols:
        if col not in df.columns:
            df[col] = "-"

    # ✅ Only process the first 10 rows for testing
    total = min(10, len(df))
    for idx, row in df.iloc[:10].iterrows():
        object_id = str(row["object_id"])
        file_path = os.path.join(HTML_DIR, f"{object_id}.html")

        if os.path.exists(file_path):
            parsed = parse_html(file_path)
            df.at[idx, "Title"] = parsed[0]
            df.at[idx, "Stars"] = parsed[1]
            df.at[idx, "No. of reviews"] = parsed[2]
            df.at[idx, "No. of bedrooms"] = parsed[3]
            df.at[idx, "Size [m2]"] = parsed[4]
            df.at[idx, "Max people"] = parsed[5]
            df.at[idx, "Features"] = parsed[6]
            df.at[idx, "Latitude"] = parsed[7]
            df.at[idx, "Longitude"] = parsed[8]
            df.at[idx, "Description"] = parsed[9]
        else:
            print(f"⚠️ No HTML file found for {object_id}")

        print(f"✅ Processed {idx+1}/{total} rows")

    # Save enriched Excel
    df.iloc[:10].to_excel(OUTPUT_FILE, index=False)
    print(f"💾 Saved Excel file with first {total} rows: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
