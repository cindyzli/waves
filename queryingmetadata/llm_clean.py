import os
import pandas as pd
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

#this prompt sucks, havent run yet. need a clearer idea of what llm purpose is because think everything can be done through first filtering?
#showing the thumbnail is too expensive i think
PROJECT_PROMPT = """
You are helping filter YouTube videos for a dataset of real piano playing showing hands to later match with sheet music.
Keep only videos that look like people playing piano pieces (at home not in concerts) that show their hands in either a birds eye view of from the side. Or any view you can see the hands on the keys.
Exclude tutorials, lessons, Synthesia visualizations, sheet music explainers, or channels dedicated to teaching.
If there's sheet music linked in the description, definitely keep it if it's a free resource like MuseScore or IMSLP or Google Drive or PDF, definitely remove it if it's someone's personaly website or paid site like MusicNotes or SheetMusicPlus.
Return YES if the video is relevant, NO otherwise.
"""

def is_relevant(title: str, description: str, thumbnail: str = None) -> bool:
    user_content = f"Title: {title}\nDescription: {description}\nThumbnail: {thumbnail or 'N/A'}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": PROJECT_PROMPT},
            {"role": "user", "content": user_content}
        ],
        max_tokens=5
    )

    answer = response.choices[0].message.content.strip().lower()
    return answer.startswith("yes")

def sift_videos(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    results = []
    for _, row in df.iterrows():
        title = str(row.get("title", ""))
        description = str(row.get("description", ""))
        thumbnail = str(row.get("thumbnail_default_url", ""))

        if is_relevant(title, description, thumbnail):
            results.append({
                "title": title,
                "watch_url": row.get("watch_url", "")
            })

    final_df = pd.DataFrame(results)
    final_df.to_csv(output_csv, index=False)
    print(f"Filtered {len(results)} relevant videos into {output_csv}")

if __name__ == "__main__":
    sift_videos("results_cleaned_python.csv", "final.csv")
