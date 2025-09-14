import pandas as pd

def extract_urls(input_csv: str, output_file: str, url_column: str = "watch_url"):
    df = pd.read_csv(input_csv)
    if url_column not in df.columns:
        raise ValueError(f"Column '{url_column}' not found in CSV. Available: {list(df.columns)}")

    urls = df[url_column].dropna().unique()

    with open(output_file, "w", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")

    print(f"Extracted {len(urls)} unique URLs into {output_file}")

if __name__ == "__main__":
    extract_urls("results_cleaned_python.csv", "final_urls.txt")
