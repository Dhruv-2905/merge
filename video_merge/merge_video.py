import os
import time
import requests
from moviepy.editor import VideoFileClip, concatenate_videoclips
from gcp_upload import upload_files_to_buckets

def fetch_video_urls(api_url):
    """Fetch video URLs and other details from the specified API."""
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        video_data = response.json()
        print(f"API response: {video_data}")
        urls = video_data.get('mp4_urls', [])
        id = video_data.get('id')
        channel_code = video_data.get('channel_code')
        start_trim = video_data.get('clip_start_time', 0)
        end_trim = video_data.get('clip_end_time', 0)
        start_time = video_data.get('substory_start_time')
        end_time = video_data.get('substory_end_time')
        print(f"Fetched URLs: {urls}")
        return urls, id, channel_code, start_trim, end_trim, start_time, end_time
    except Exception as e:
        print(f"Error fetching video URLs from API: {e}")
        return None

def download_clips(clip_urls, temp_folder):
    """Download videos from the provided URLs and save them in the temp folder."""
    os.makedirs(temp_folder, exist_ok=True)
    
    # Remove any old video files from temp_folder
    for file in os.listdir(temp_folder):
        file_path = os.path.join(temp_folder, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    video_files = []
    for url in clip_urls:
        local_filename = os.path.join(temp_folder, os.path.basename(url))
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            video_files.append(local_filename)
        except Exception as e:
            print(f"Error downloading video from {url}: {e}")
    return video_files

def update_database(api_url, id, merged_clip_url):
    """Update the database with the merged video URL."""
    patch_url = f"{api_url}?id={id}&merged_clip={merged_clip_url}"
    try:
        response = requests.patch(patch_url, headers={'accept': 'application/json'})
        if response.status_code == 200:
            print(f"Database updated successfully with merged clip URL: {merged_clip_url}")
        else:
            print(f"Error updating database. Status code: {response.status_code}")
            print(f"Response content: {response.content}")
    except Exception as e:
        print(f"Error updating database: {e}")

def merge_videos(temp_folder, merge_folder, api_url):
    """Merge downloaded video files with trimming and save the output."""
    while True:
        # Fetch the clips to merge
        result = fetch_video_urls(api_url)
        
        if result is None:
            print("Failed to fetch video URLs from the API.")
            time.sleep(10)  # Wait for 60 seconds before retrying
            continue
        
        urls, id, channel_code, start_trim, end_trim, start_time, end_time = result
        
        if not urls:
            print("No clips to merge.")
            time.sleep(10)  # Wait for 60 seconds before retrying
            continue
        
        # Download clips to the temp folder
        video_files = download_clips(urls, temp_folder)
        
        # Create the merge folder if it does not exist
        os.makedirs(merge_folder, exist_ok=True)
        
        if len(video_files) == 0:
            print("No video files found in the temp folder.")
            time.sleep(10)  # Wait for 60 seconds before retrying
            continue
        
        # Load video files into a list of VideoFileClip objects
        video_clips = []
        for i, video_file in enumerate(video_files):
            try:
                clip = VideoFileClip(video_file)
                if len(video_files) == 1:
                    # If there's only one video, trim the start and end
                    clip = clip.subclip(start_trim, clip.duration - end_trim)
                else:
                    if i == 0:
                        clip = clip.subclip(start_trim, clip.duration)  # Trim the start of the first video
                    if i == len(video_files) - 1:
                        clip = clip.subclip(0, clip.duration - end_trim)  # Trim the end of the last video
                video_clips.append(clip)
            except Exception as e:
                print(f"Error processing video file {video_file}: {e}")
        
        # Concatenate all video clips if there are multiple videos
        if len(video_clips) > 1:
            final_clip = concatenate_videoclips(video_clips)
        else:
            final_clip = video_clips[0]
        
        # Extract time portion from start_time and end_time for filename
        start_time_str = start_time.split('T')[-1].replace(':', '-').split('.')[0]
        end_time_str = end_time.split('T')[-1].replace(':', '-').split('.')[0]
        
        # Construct the output filename
        output_filename = f"{channel_code}_{start_time_str}_{end_time_str}.mp4"
        output_path = os.path.join(merge_folder, output_filename)
        
        try:
            final_clip.write_videofile(output_path, codec='libx264', audio_codec='aac')
        except Exception as e:
            print(f"Error writing final video file {output_path}: {e}")
        
        # Explicitly close all clips
        for clip in video_clips:
            clip.close()
        
        # Delete the original video files from the temp folder
        for video_file in video_files:
            os.remove(video_file)
        
        # Upload the merged video to Google Cloud Storage
        try:
            cloud_url = upload_files_to_buckets(output_path, 'Merge_video')
            print(f"Uploaded merged video to {cloud_url}")
            
            # Update the database with the URL of the merged video
            update_database('https://tabsons-fastapi-g55rbik64q-el.a.run.app/set_merged_clip_status', id, cloud_url)
            
            # Delete the merged video file from the local merge_folder
            os.remove(output_path)
            print(f"Removed merged video file: {output_path}")
            
        except Exception as e:
            print(f"Error uploading merged video: {e}")
        
        # Wait for 60 seconds before checking for new data
        time.sleep(10)

# Example usage
temp_folder = 'single_clip'
merge_folder = 'merge_clip'
api_url = 'https://tabsons-fastapi-g55rbik64q-el.a.run.app/get_clips_for_merging/'
merge_videos(temp_folder, merge_folder, api_url)
