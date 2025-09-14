import pandas as pd

BAD_WORDS = ["tutorial", "lesson", "synthesia", "guitar", "playlist", 
             "sight reading", "sight read", "how to", "learn", "explain"]
REQUIRED_WORDS = ["piano"]

def contains_bad_word(text: str) -> bool:
    """Return True if the text contains any bad word."""
    if not isinstance(text, str):
        return False
    lower = text.lower()
    return any(bad in lower for bad in BAD_WORDS)

def contains_required_word(text: str) -> bool:
    """Return True if the text contains at least one required word."""
    if not isinstance(text, str):
        return False
    lower = text.lower()
    return any(req in lower for req in REQUIRED_WORDS)

def clean_csv(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    bad_mask = df["title"].apply(contains_bad_word) | df["description"].apply(contains_bad_word)
    required_mask = df["title"].apply(contains_required_word) | df["description"].apply(contains_required_word)
    mask = ~bad_mask & required_mask
    cleaned = df[mask]

    cleaned.to_csv(output_csv, index=False)
    print(f"Original rows: {len(df)}")
    print(f"Cleaned rows: {len(cleaned)}")
    print(f"Saved cleaned data to {output_csv}")

if __name__ == "__main__":
    clean_csv("results.csv", "results_cleaned_python.csv")
