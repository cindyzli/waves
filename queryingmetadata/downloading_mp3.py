import os
import pandas as pd
import yt_dlp

def download_mp3(url: str, title: str, out_dir: str = "downloads"):
    os.makedirs(out_dir, exist_ok=True)
    
    safe_title = "".join(c for c in title if c.isalnum() or c in " -_").rstrip()
    output_path = os.path.join(out_dir, f"{safe_title}.%(ext)s")

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": output_path,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
        "quiet": False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        print(f"Downloading: {title}")
        ydl.download([url])


def download_from_csv(csv_file: str, out_dir: str = "downloads"):
    df = pd.read_csv(csv_file)
    for _, row in df.iterrows():
        title = str(row.get("title", "untitled"))
        url = row.get("watch_url")
        if pd.notna(url):
            try:
                download_mp3(url, title, out_dir)
            except Exception as e:
                print(f"Failed to download {title} ({url}): {e}")


if __name__ == "__main__":
    download_from_csv("final.csv")
