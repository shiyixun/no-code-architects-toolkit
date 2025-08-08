# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



import os
import json
import subprocess
import logging
import uuid
from services.file_management import download_file, get_filename_from_url
from services.cloud_storage import upload_file
from config import LOCAL_STORAGE_PATH
import requests

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def time_to_seconds(time_str):
    """
    Convert a time string in format HH:MM:SS[.mmm] to seconds.
    
    Args:
        time_str (str): Time string
        
    Returns:
        float: Time in seconds
    """
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
        elif len(parts) == 2:
            minutes, seconds = parts
            return int(minutes) * 60 + float(seconds)
        else:
            return float(time_str)
    except ValueError:
        raise ValueError(f"Invalid time format: {time_str}. Expected HH:MM:SS[.mmm]")

def split_video(video_url, splits, job_id=None, video_codec='libx264', video_preset='medium', 
               video_crf=23, audio_codec='aac', audio_bitrate='128k'):
    """
    Splits a video file into multiple segments with customizable encoding settings.
    
    Args:
        video_url (str): URL of the video file to split
        splits (list): List of dictionaries with 'start' and 'end' timestamps
        job_id (str, optional): Unique job identifier
        video_codec (str, optional): Video codec to use for encoding (default: 'libx264')
        video_preset (str, optional): Encoding preset for speed/quality tradeoff (default: 'medium')
        video_crf (int, optional): Constant Rate Factor for quality (0-51, default: 23)
        audio_codec (str, optional): Audio codec to use for encoding (default: 'aac')
        audio_bitrate (str, optional): Audio bitrate (default: '128k')
        
    Returns:
        tuple: (list of output file paths, input file path)
    """
    logger.info(f"Starting video split operation for {video_url}")
    if not job_id:
        job_id = str(uuid.uuid4())

    # Change video_url extension to .json then check if the JSON file exists in Cloud Storage
    video_json_url = video_url.rsplit('.', 1)[0] + '.json'
    try:
        # Try to check if the JSON file is accessible before downloading
        """
            video_path (str, optional): Local path to video file. If provided and exists, use it directly.
            video_splits (list, optional): List of existing split segments to check for duplicates.
        """
        response = requests.head(video_json_url)
        if response.status_code == 200:
            video_json_path = download_file(video_json_url, os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input"))
            logger.info(f"Downloaded video_json to local file: {video_json_path}")
        else:
            logger.warning(f"JSON file not found at {video_json_url}, status code: {response.status_code}")
            video_json_path = None
    except Exception as e:
        logger.warning(f"Failed to check or download JSON file: {str(e)}")
        video_json_path = None

    # Get video_json_file name from URL
    video_json_file = get_filename_from_url(video_json_url)

    # Read the JSON file and parse to get the video URL and splits
    if video_json_path:
        try:
            with open(video_json_path, 'r') as f:
                video_json = json.load(f)
                if not isinstance(video_json, dict):
                    logger.error("JSON file does not contain a valid object, using default values")
                    video_json = {}
                    video_path = None
                    video_splits = []
                else:
                    video_path = video_json.get('video_path', None)
                    video_splits = video_json.get('video_splits', [])
                    if not isinstance(video_splits, list):
                        logger.error("video_splits in JSON file is not a list, using default value []")
                        video_splits = []
                logger.info(f"Loaded video URL: {video_path} and video splits from JSON file")
        except Exception as e:
            logger.error(f"Failed to read or parse JSON file: {str(e)}, using default values")
            video_json = {}
            video_path = None
            video_splits = []
    else:
        video_json = {}
        video_path = None
        video_splits = []
        logger.info("No JSON file found, using provided video URL and splits")

    # Only download if video_path is None or file does not exist
    if video_path is None or (video_path and not os.path.exists(video_path)):
        input_filename = download_file(video_url, os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input"))
        logger.info(f"Downloaded video to local file: {input_filename}")
    else:
        input_filename = video_path
        logger.info(f"Using provided local video file: {input_filename}")
    
    temp_files = []
    
    try:
        # Get the file extension
        _, ext = os.path.splitext(input_filename)
        
        # Get the duration of the input file
        probe_cmd = [
            'ffprobe', 
            '-v', 'error', 
            '-show_entries', 'format=duration', 
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_filename
        ]
        duration_result = subprocess.run(probe_cmd, capture_output=True, text=True)
        
        try:
            file_duration = float(duration_result.stdout.strip())
            logger.info(f"File duration: {file_duration} seconds")
        except (ValueError, IndexError):
            logger.warning("Could not determine file duration, using a large value")
            file_duration = 86400  # 24 hours as a fallback

        # Validate and process splits
        valid_splits = []
        for i, split in enumerate(splits):
            try:
                start_seconds = time_to_seconds(split['start'])
                end_seconds = time_to_seconds(split['end'])
                
                # Validate split times
                if start_seconds >= end_seconds:
                    logger.warning(f"Invalid split {i+1}: start time ({split['start']}) must be before end time ({split['end']}). Skipping.")
                    continue
                
                if start_seconds < 0:
                    logger.warning(f"Split {i+1} start time {split['start']} is negative, using 0 instead")
                    start_seconds = 0
                    
                if end_seconds > file_duration:
                    logger.warning(f"Split {i+1} end time {split['end']} exceeds file duration, using file duration instead")
                    end_seconds = file_duration
                    
                # Check if split is valid
                if start_seconds < end_seconds:
                    valid_splits.append((i, start_seconds, end_seconds, split))
            except ValueError as e:
                logger.warning(f"Error processing split {i+1}: {str(e)}. Skipping.")
        
        if not valid_splits:
            raise ValueError("No valid split segments specified")
            
        logger.info(f"Processing {len(valid_splits)} valid splits")
        
        # Process each split
        for index, (split_index, start_seconds, end_seconds, split_data) in enumerate(valid_splits):
            # Create output filename for this split
            output_filename = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_split_{split_index+1}{ext}")
            
            # Check if split is in video_splits
            is_duplicate = any(
                split_data['start'] == vs.get('start') and split_data['end'] == vs.get('end')
                for vs in video_splits or []
            )
            if is_duplicate:
                logger.info(f"Split {split_index+1} is a duplicate of an existing split, skipping encoding")
                continue

            # Create FFmpeg command to extract the segment
            cmd = [
                'ffmpeg',
                '-i', input_filename,
                '-ss', str(start_seconds),
                '-to', str(end_seconds),
                '-c:v', video_codec,
                '-preset', video_preset,
                '-crf', str(video_crf),
                '-c:a', audio_codec,
                '-b:a', audio_bitrate,
                '-avoid_negative_ts', 'make_zero',
                output_filename
            ]
            
            logger.info(f"Running FFmpeg command for split {split_index+1}: {' '.join(cmd)}")
            
            # Run the FFmpeg command
            process = subprocess.run(cmd, capture_output=True, text=True)
            
            if process.returncode != 0:
                logger.error(f"Error processing split {split_index+1}: {process.stderr}")
                raise Exception(f"FFmpeg error for split {split_index+1}: {process.stderr}")
            
            # Log the successful creation of the split file
            logger.info(f"Successfully created split {split_index+1}: {output_filename}")

            # Upload the output file to cloud storage
            cloud_url = upload_file(output_filename)
            if not cloud_url:
                raise Exception(f"Failed to upload split {split_index+1} to cloud storage")
            
            # Add the split data to video_splits
            video_splits.append({
                "split_index": split_index+1,
                "file_url": cloud_url,
                "start": split_data["start"],
                "end": split_data["end"]
            })
            # Sort video_splits by split_index ascending
            video_splits.sort(key=lambda x: x["split_index"])

            # upload the updated video_splits JSON to cloud storage
            # Write video_splits_json to a temporary local file before uploading
            video_json['video_path'] = input_filename
            video_json['video_splits'] = video_splits
            video_splits_json = json.dumps(video_json, indent=4)
            temp_json_path = os.path.join(LOCAL_STORAGE_PATH, video_json_file)
            with open(temp_json_path, 'w') as json_f:
                json_f.write(video_splits_json)
            cloud_url = upload_file(temp_json_path)
            if not cloud_url:
                raise Exception(f"Failed to upload video splits JSON to cloud storage")
            
            # Log the successful upload of the JSON file
            logger.info(f"Uploaded video JSON to cloud storage: {cloud_url}")

            # Add the output filename and JSON path to the list of temporary files
            temp_files.append(output_filename)
            temp_files.append(temp_json_path)
            
            # Remove the temporary local JSON file after upload
            os.remove(temp_json_path)

            # Remove the local file after upload
            os.remove(output_filename)
        
        # Return the list of output files and the input filename
        return video_splits, input_filename

    except Exception as e:
        logger.error(f"Video split operation failed: {str(e)}")
        
        # Clean up all temporary files if they exist
        if 'input_filename' in locals() and os.path.exists(input_filename):
            os.remove(input_filename)

        for f in temp_files:
            if os.path.exists(f):
                os.remove(f)

        raise