import os
import librosa
import numpy as np
import pretty_midi
from music21 import converter
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import soundfile as sf
from mt3 import models
from mt3 import infer
from mt3 import data

# Convert MP3 to WAV
def convert_mp3_to_wav(mp3_path, wav_path):
    y, sr = librosa.load(mp3_path, sr=None)
    sf.write(wav_path, y, sr)
    print(f"Converted {mp3_path} to {wav_path}")

# Transcribe WAV to MIDI (placeholder for MT3)
def transcribe_audio_to_midi(wav_path, midi_output_path="transcription.mid"):
    print("Loading MT3 model...")
    model = models.load_model()
    print(f"Loading audio {wav_path}")
    audio_ds = data.load_audio([wav_path])
    print("Transcribing...")
    note_sequence = infer.transcribe(model, audio_ds)
    print(f"Writing transcription to {midi_output_path}")
    note_sequence[0].to_midi_file(midi_output_path)
    
    return pretty_midi.PrettyMIDI(midi_output_path)

# Convert MusicXML to MIDI
def convert_musicxml_to_midi(musicxml_path, midi_output_path="score.mid"):
    score = converter.parse(musicxml_path)
    score.write("midi", fp=midi_output_path)
    return pretty_midi.PrettyMIDI(midi_output_path)

# Extract MIDI features
def extract_midi_features(midi):
    notes = []
    for instrument in midi.instruments:
        for note in instrument.notes:
            notes.append((note.start, note.pitch))
    notes.sort()  # Sort by onset
    onsets = np.array([n[0] for n in notes])
    pitches = np.array([n[1] for n in notes])
    return onsets, pitches

# Align two MIDIs with DTW
def align_midis(score_midi, perf_midi):
    print("Aligning MIDI files...")
    score_onsets, score_pitches = extract_midi_features(score_midi)
    perf_onsets, perf_pitches = extract_midi_features(perf_midi)
    
    score_features = np.column_stack((score_pitches, score_onsets / max(score_onsets)))
    perf_features = np.column_stack((perf_pitches, perf_onsets / max(perf_onsets)))
    
    distance, path = fastdtw(score_features, perf_features, dist=euclidean)
    alignment = list(zip(*path))
    return alignment, score_onsets, perf_onsets, score_pitches, perf_pitches

# Save alignment to CSV
def save_alignment(alignment, score_onsets, perf_onsets, score_pitches, perf_pitches, output_path):
    with open(output_path, 'w') as f:
        f.write("Score_Onset,Score_Pitch,Performance_Onset,Performance_Pitch\n")
        for s_idx, p_idx in alignment:
            f.write(f"{score_onsets[s_idx]},{score_pitches[s_idx]},{perf_onsets[p_idx]},{perf_pitches[p_idx]}\n")
    print(f"Saved alignment to {output_path}")

# Pipeline
mp3_path = "downloads/lovedream.mp3"
musicxml_path = "downloads/Liebestraum_No._3_in_A_Major.mxl"
wav_path = "lovedream.wav"
output_alignment = "lovedream_alignment.csv"

convert_mp3_to_wav(mp3_path, wav_path)
perf_midi = transcribe_audio_to_midi(wav_path)  # Replace with MT3 transcription
score_midi = convert_musicxml_to_midi(musicxml_path)
alignment, score_onsets, perf_onsets, score_pitches, perf_pitches = align_midis(score_midi, perf_midi)
save_alignment(alignment, score_onsets, perf_onsets, score_pitches, perf_pitches, output_alignment)
