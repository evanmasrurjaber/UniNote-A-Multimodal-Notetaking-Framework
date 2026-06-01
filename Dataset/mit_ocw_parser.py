import os
import requests
import yt_dlp
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

def extract_lecture_number(text):
    """
    Attempts to pull the lecture number from a filename or video title.
    Matches formats like: 'L01', 'lec 1', 'Lecture 01', '1. Video Title'
    """
    match = re.search(r'(?:lec|lecture|l)[_ -]?0*(\d+)', text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    
    match = re.search(r'^0*(\d+)\.', text)
    if match:
        return int(match.group(1))
        
    return None

def download_ocw_notes(ocw_notes_url, notes_folder, course_id):
    """
    Scrapes PDFs by targeting an OCW Lecture Notes index page and deep-scanning its resources.
    """
    print(f"\n📝 Initiating Deep-Scan PDF Sequence from Index Page...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    valid_lecture_numbers = set()
    
    try:
        response = requests.get(ocw_notes_url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to load notes page: {e}")
        return valid_lecture_numbers

    soup = BeautifulSoup(response.text, 'html.parser')
    
    pdf_links = set()
    resource_pages = set()

    # Pass 1: Surface scan the index page
    for link in soup.find_all('a', href=True):
        full_url = urljoin(ocw_notes_url, link['href'])
        link_text = link.text.strip().lower()
        clean_url = full_url.split('?')[0]
        
        # Direct PDF found on the main page
        if clean_url.lower().endswith('.pdf'):
            pdf_links.add(full_url)
            
        # Resource pages hosted on OCW (we look for lecture-related keywords)
        elif '/resources/' in full_url.lower():
            if any(keyword in full_url.lower() or keyword in link_text for keyword in ['lec', 'lecture', 'note', 'session']):
                resource_pages.add(full_url)

    # Pass 2: Deep crawl the discovered resource pages
    if resource_pages:
        print(f"🕵️ Deep scanning {len(resource_pages)} potential lecture resource pages...")
        for res_url in resource_pages:
            try:
                res_page = requests.get(res_url, headers=headers, timeout=10)
                res_soup = BeautifulSoup(res_page.text, 'html.parser')
                
                for sub_link in res_soup.find_all('a', href=True):
                    sub_url = urljoin(res_url, sub_link['href'])
                    sub_clean = sub_url.split('?')[0]
                    
                    # Look for actual files or explicit download buttons
                    if sub_clean.lower().endswith('.pdf') or '?download=true' in sub_url.lower():
                        pdf_links.add(sub_url)
            except Exception:
                pass # Silently skip timeouts to keep pipeline moving

    # De-duplicate to prevent downloading the same file multiple times via different URL parameters
    unique_pdfs = {}
    for url in pdf_links:
        filename = unquote(url.split('?')[0].split('/')[-1])
        if filename.endswith('.pdf'):
            unique_pdfs[filename] = url

    print(f"🔍 Discovered {len(unique_pdfs)} unique PDF documents.")

    # Pass 3: Download and Extract Lecture Numbers
    for filename, pdf_url in unique_pdfs.items():
        # Strictly exclude problem sets, solutions, exams, and reviews
        if any(skip_word in filename.lower() for skip_word in ['review', 'quiz', 'exam', 'midterm', 'final', 'prob', 'ps', 'sol', 'recitation', 'r0']):
            continue

        lec_num = extract_lecture_number(filename)
        if lec_num is not None:
            valid_lecture_numbers.add(lec_num)
            clean_filename = f"{course_id}_Lec{lec_num:02d}.pdf"
            filepath = os.path.join(notes_folder, clean_filename)
            
            if not os.path.exists(filepath):
                try:
                    with requests.get(pdf_url, stream=True, timeout=20) as r:
                        r.raise_for_status()
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    f.write(chunk)
                    print(f"✅ Saved PDF: {filename} (Identified as Lecture {lec_num})")
                except Exception as e:
                    print(f"❌ Failed to download {filename}: {str(e)}")
            else:
                print(f"⏩ Skipping (already exists): {filename} (Identified as Lecture {lec_num})")

    return valid_lecture_numbers

def download_matched_videos(playlist_url, course_id, video_folder, valid_lecture_numbers):
    """
    Scrapes playlist metadata, filters out unmatched videos, and downloads the rest.
    """
    print(f"\n🎥 Analyzing YouTube Playlist for Matched Videos...")
    
    extract_opts = {'extract_flat': True, 'quiet': True}
    
    with yt_dlp.YoutubeDL(extract_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)
        
    if 'entries' not in playlist_info:
        print("❌ Could not extract playlist entries.")
        return

    matched_video_urls = []
    
    for entry in playlist_info['entries']:
        title = entry.get('title', '')
        url = entry.get('url')
        
        # Skip reviews and non-lectures
        if any(skip_word in title.lower() for skip_word in ['review', 'quiz', 'exam', 'midterm', 'final', 'problem session', 'recitation']):
            print(f"⏩ Skipping (Review/Session): {title}")
            continue
            
        lec_num = extract_lecture_number(title)
        
        if lec_num is not None and lec_num in valid_lecture_numbers:
            print(f"✅ Match Found: Lecture {lec_num} -> {title}")
            matched_video_urls.append(url)
        else:
            print(f"❌ No matching PDF found for: {title} (Extracted Num: {lec_num}) - Skipping.")

    print(f"\n📥 Proceeding to download {len(matched_video_urls)} matched videos...")

    download_opts = {
        'format': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best',
        'outtmpl': os.path.join(video_folder, f'{course_id}_%(title)s.%(ext)s'),
        'writesubtitles': False,
        'writeinfojson': True, 
        'ignoreerrors': True,
    }

    if matched_video_urls:
        with yt_dlp.YoutubeDL(download_opts) as ydl:
            ydl.download(matched_video_urls)
    else:
        print("⚠️ No matched videos to download.")

def build_strict_dataset(course_id, youtube_playlist_url, ocw_notes_url, base_path="data/raw_dataset/"):
    course_folder = os.path.join(base_path, course_id)
    video_folder = os.path.join(course_folder, "videos")
    notes_folder = os.path.join(course_folder, "notes")

    os.makedirs(video_folder, exist_ok=True)
    os.makedirs(notes_folder, exist_ok=True)

    print(f"🎯 Target Course: {course_id}")
    print(f"📁 Root: {course_folder}")

    valid_lecture_numbers = set()
    if ocw_notes_url:
        valid_lecture_numbers = download_ocw_notes(ocw_notes_url, notes_folder, course_id)
        
    if not valid_lecture_numbers:
        print("❌ No valid numbered PDFs found. Halting video extraction to prevent mismatched data.")
        return

    if youtube_playlist_url:
        download_matched_videos(youtube_playlist_url, course_id, video_folder, valid_lecture_numbers)
        
    print(f"\n🎉 Strict dataset collection for {course_id} complete!")

if __name__ == "__main__":
    COURSE_ID = "6.7960_Deep_Learning"
    
    YOUTUBE_URL = "https://youtube.com/playlist?list=PLUl4u3cNGP63URZnh5iqBzDTDYPUTQT-8&si=Bufu5MrwIQIpoq1-"
    
    # Updated to the new targeted Lecture Notes page
    OCW_URL = "https://ocw.mit.edu/courses/6-7960-deep-learning-fall-2024/resources/lecture-notes/"
    
    build_strict_dataset(COURSE_ID, YOUTUBE_URL, OCW_URL)