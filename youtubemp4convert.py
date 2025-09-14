import yt_dlp
from pydub import AudioSegment
import os
#only mp3, need mp4 -> maybe can do it once find musecore that matches??
def youtube_to_mp3(url, output_folder="downloads"):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'{output_folder}/%(title)s.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


url = "https://www.youtube.com/watch?v=oHTuKM8YuJI&list=RDoHTuKM8YuJI&start_radio=1&ab_channel=ichizupiano" 
youtube_to_mp3(url)
