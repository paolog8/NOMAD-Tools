"""
NOMAD Data Retrieval Module

This module provides functions for retrieving and processing data from NOMAD,
specifically focused on HySprint samples.
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from tqdm.notebook import tqdm  # For nice progress bars in notebooks
from pathlib import Path

# Cache configuration
CACHE_DIR = Path('.nomad_cache')
CACHE_CONFIG = {
    'entries': {'expire_hours': 24},  # Cache entries for 24 hours
    'users': {'expire_hours': 168},   # Cache user details for 1 week
    'uploads': {'expire_hours': 48}    # Cache upload details for 48 hours
}

def ensure_cache_dir():
    """Create cache directory if it doesn't exist"""
    CACHE_DIR.mkdir(exist_ok=True)
    for cache_type in CACHE_CONFIG.keys():
        (CACHE_DIR / cache_type).mkdir(exist_ok=True)

def get_cache_path(cache_type: str, key: str) -> Path:
    """Get the path for a cached item"""
    return CACHE_DIR / cache_type / f"{key}.json"

def save_to_cache(cache_type: str, key: str, data: Any):
    """Save data to cache with timestamp"""
    ensure_cache_dir()
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'data': data
    }
    with open(get_cache_path(cache_type, key), 'w') as f:
        json.dump(cache_data, f)

def load_from_cache(cache_type: str, key: str) -> Optional[Any]:
    """Load data from cache if not expired"""
    cache_path = get_cache_path(cache_type, key)
    if not cache_path.exists():
        return None
        
    try:
        with open(cache_path) as f:
            cache_data = json.load(f)
            
        timestamp = datetime.fromisoformat(cache_data['timestamp'])
        expire_hours = CACHE_CONFIG[cache_type]['expire_hours']
        if datetime.now() - timestamp > timedelta(hours=expire_hours):
            return None
            
        return cache_data['data']
    except:
        return None

def clear_cache(cache_type: Optional[str] = None):
    """Clear all cache or specific cache type"""
    if cache_type:
        cache_dir = CACHE_DIR / cache_type
        if cache_dir.exists():
            for cache_file in cache_dir.glob('*.json'):
                cache_file.unlink()
    else:
        if CACHE_DIR.exists():
            for cache_type_dir in CACHE_DIR.glob('*'):
                if cache_type_dir.is_dir():
                    for cache_file in cache_type_dir.glob('*.json'):
                        cache_file.unlink()

def get_cache_stats() -> Dict[str, Dict[str, Any]]:
    """Get statistics about the cache"""
    stats = {}
    for cache_type in CACHE_CONFIG.keys():
        cache_dir = CACHE_DIR / cache_type
        if cache_dir.exists():
            files = list(cache_dir.glob('*.json'))
            total_size = sum(f.stat().st_size for f in files)
            oldest = min((f.stat().st_mtime for f in files), default=None)
            newest = max((f.stat().st_mtime for f in files), default=None)
            
            stats[cache_type] = {
                'count': len(files),
                'size_kb': total_size / 1024,
                'oldest': datetime.fromtimestamp(oldest).isoformat() if oldest else None,
                'newest': datetime.fromtimestamp(newest).isoformat() if newest else None
            }
    return stats

def get_user_details(client, user_id: str) -> Dict[str, Any]:
    """
    Get user details from NOMAD API
    
    Parameters:
    -----------
    client: NomadClient
        Authenticated NOMAD API client
    user_id: str
        User ID to look up
        
    Returns:
    --------
    dict
        User details including name, email, etc.
    """
    try:
        response = client.make_request('get', f'users/{user_id}')
        return response.get('data', {})
    except Exception as e:
        print(f"Error getting user details for {user_id}: {str(e)}")
        return {}

def get_hysprint_data(client, max_entries: Optional[int] = 500) -> Optional[pd.DataFrame]:
    """
    Retrieve HySprint sample data from NOMAD with caching
    
    Parameters:
    -----------
    client: NomadClient
        Authenticated NOMAD API client
    max_entries: Optional[int]
        Maximum number of entries to retrieve, None to retrieve all available entries
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing HySprint sample data
    """
    try:
        # Try to load from cache first
        cache_key = f"hysprint_data_{max_entries}"
        cached_data = load_from_cache('entries', cache_key)
        if cached_data is not None:
            print("Loading data from cache...")
            return pd.DataFrame(cached_data)

        print(f"Retrieving {'all' if max_entries is None else f'up to {max_entries}'} HySprint samples...")
        
        # First try with admin access, then fall back to visible
        access_levels = ["admin", "visible"]
        query_payload = None
        response = None
        
        for access_level in access_levels:
            try:
                query_payload = {
                    "owner": access_level,
                    "query": {
                        "and": [
                            {"results.eln.sections:any": ["HySprint_Sample"]},
                            {"quantities:all": ["data"]},
                        ]
                    },
                    "pagination": {
                        "page_size": 100,  # Always use maximum page size allowed
                        "page": 1
                    }
                }
                
                print(f"Trying to retrieve samples with {access_level} access...")
                response = client.make_request("post", "entries/query", json_data=query_payload)
                if response:
                    print(f"Successfully retrieved samples with {access_level} access")
                    break
                    
            except Exception as e:
                if access_level == "admin":
                    print(f"Admin access failed, falling back to visible access...")
                    continue
                else:
                    raise Exception(f"Failed to retrieve samples with both admin and visible access: {str(e)}")
        
        if not response or 'data' not in response:
            raise ValueError("No data received from NOMAD API")
            
        entries = response['data']
        total_entries = response.get('pagination', {}).get('total', 0)
        print(f"Found {total_entries} total entries")
        
        # Calculate total pages needed
        total_pages = (total_entries + 99) // 100  # Ceiling division
        if max_entries is not None:
            total_pages = min((max_entries + 99) // 100, total_pages)
        
        # If max_entries is None, retrieve all entries
        all_entries = entries.copy()
        
        # Only fetch additional pages if needed
        if total_pages > 1:
            print(f"Fetching data pages:")
            # Create progress bar for page fetching
            for page in tqdm(range(2, total_pages + 1), desc="Fetching pages", total=total_pages-1, unit="page"):
                query_payload["pagination"]["page"] = page
                response = client.make_request("post", "entries/query", json_data=query_payload)
                if response and 'data' in response:
                    page_entries = response['data']
                    all_entries.extend(page_entries)
                    if max_entries is not None and len(all_entries) >= max_entries:
                        all_entries = all_entries[:max_entries]
                        break
                        
        entries = all_entries
        print(f"Retrieved {len(entries)} entries. Now processing author details...")
        
        # Cache for user details to avoid repeated API calls
        user_cache = {}
        
        # Process entries into a list of dictionaries
        samples_data = []
        print("Processing author details for each sample:")
        for entry in tqdm(entries, desc="Processing samples", total=len(entries), unit="sample"):
            # Get upload details to get author information
            upload_id = entry.get('upload_id')
            if upload_id:
                # Try to get upload details from cache
                upload_cache_key = f"upload_{upload_id}"
                upload_data = load_from_cache('uploads', upload_cache_key)
                
                if upload_data is None:
                    upload_response = client.make_request("get", f"uploads/{upload_id}")
                    upload_data = upload_response.get('data', {})
                    # Cache the upload data
                    save_to_cache('uploads', upload_cache_key, upload_data)
                
                # Get upload name
                upload_name = upload_data.get('upload_name', '')
                
                # Get author details
                author_id = upload_data.get('main_author', '')
                if author_id:
                    # Try to get author details from cache
                    author_info = load_from_cache('users', author_id)
                    if author_info is None and author_id not in user_cache:
                        author_info = get_user_details(client, author_id)
                        user_cache[author_id] = author_info
                        # Cache the author data
                        save_to_cache('users', author_id, author_info)
                    elif author_id in user_cache:
                        author_info = user_cache[author_id]
                else:
                    author_info = {}
                
                author_name = author_info.get('name', author_info.get('username', author_id))
                
                # Extract sample data
                sample_info = {
                    'upload_id': upload_id,
                    'upload_name': upload_name,
                    'sample_name': entry.get('data', {}).get('name', ''),
                    'lab_id': entry.get('data', {}).get('lab_id', ''),
                    'upload_date': upload_data.get('upload_create_time', ''),
                    'main_author_id': author_id,
                    'main_author': author_name,
                    'cell_area': entry.get('results', {}).get('properties', {}).get('optoelectronic', {}).get('solar_cell', {}).get('cell_area', 0.0),
                    'efficiency': entry.get('results', {}).get('properties', {}).get('optoelectronic', {}).get('solar_cell', {}).get('efficiency', 0.0)
                }
                samples_data.append(sample_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(samples_data)
        
        # Convert dates to datetime
        if 'upload_date' in df.columns:
            df['upload_date'] = pd.to_datetime(df['upload_date']).dt.strftime('%Y-%m-%d')
        
        # Cache the processed data
        save_to_cache('entries', cache_key, samples_data)
            
        print(f"Processing complete. Retrieved {len(df)} samples.")
        return df
        
    except Exception as e:
        print(f"Error retrieving HySprint data: {str(e)}")
        return None

def load_attributions(filename: str = 'attribution_overrides.csv') -> Dict[str, Dict[str, str]]:
    """
    Load attribution overrides from file
    
    This function loads user-defined overrides for sample attributions from a CSV file.
    Each override associates a sample (identified by upload_id) with an author,
    allowing corrections to the original NOMAD attribution data.
    
    Parameters:
    -----------
    filename: str
        Path to the attribution overrides file
        
    Returns:
    --------
    dict
        Dictionary of attribution overrides with structure:
        {
            'upload_id': {
                'author_id': 'original_or_override_id',
                'author_display_name': 'human_readable_name',
                'override_date': 'YYYY-MM-DD'
            }
        }
    """
    attributions = {}
    try:
        # Check if file exists
        import os
        
        # For backward compatibility, check both the new and old filenames
        old_filename = 'nomad_samples_with_authors.csv'
        if not os.path.exists(filename) and os.path.exists(old_filename):
            print(f"Using legacy attribution file {old_filename}")
            filename = old_filename
        elif not os.path.exists(filename):
            print(f"Attribution file {filename} not found. Starting with empty attributions.")
            return attributions
            
        # Remove comment lines before parsing with pandas
        with open(filename, 'r') as f:
            lines = f.readlines()
        
        # Filter out comment lines
        data_lines = [line for line in lines if not line.strip().startswith('#')]
        
        if not data_lines:
            print(f"No data found in attribution file {filename}.")
            return attributions
            
        # Write filtered content to a temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
            temp_filename = temp.name
            temp.writelines(data_lines)
            
        # Load the filtered CSV
        df = pd.read_csv(temp_filename)
        
        # Clean up the temporary file
        os.unlink(temp_filename)
            
        # Convert to dictionary with improved field names
        for _, row in df.iterrows():
            if 'upload_id' in row:
                attribution_data = {
                    'override_date': row.get('override_date', datetime.now().strftime('%Y-%m-%d'))
                }
                
                # Handle the author ID/name fields (with backwards compatibility)
                if 'author_id' in row:
                    attribution_data['author_id'] = row['author_id']
                elif 'main_author' in row:  # Legacy field name
                    attribution_data['author_id'] = row['main_author']
                
                # Handle the display name field (with backwards compatibility)
                if 'author_display_name' in row:
                    attribution_data['author_display_name'] = row['author_display_name']
                elif 'main_author_name' in row:  # Legacy field name
                    attribution_data['author_display_name'] = row['main_author_name']
                elif 'author_id' in attribution_data:  # Fallback to ID if no name is available
                    attribution_data['author_display_name'] = attribution_data['author_id']
                    
                attributions[row['upload_id']] = attribution_data
                
        print(f"Loaded {len(attributions)} attribution overrides")
        return attributions
        
    except Exception as e:
        print(f"Error loading attributions: {str(e)}")
        return attributions

def save_attributions(attributions: Dict[str, Dict[str, str]], filename: str = 'attribution_overrides.csv') -> bool:
    """
    Save attribution overrides to file
    
    This function persists the current state of attribution overrides to a CSV file.
    Each row represents a sample whose author attribution has been manually corrected.
    
    Parameters:
    -----------
    attributions: dict
        Dictionary of attribution overrides
    filename: str
        Path to save the attribution overrides file
        
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    try:
        # Import os here to ensure it's available
        import os
        
        # Convert to DataFrame with improved field names
        data = []
        for upload_id, attr_info in attributions.items():
            # Handle both the new field names and legacy field names
            author_id = attr_info.get('author_id', attr_info.get('main_author', ''))
            author_display_name = attr_info.get('author_display_name', 
                                      attr_info.get('main_author_name', author_id))
            
            data.append({
                'upload_id': upload_id,
                'author_id': author_id,
                'author_display_name': author_display_name,
                'override_date': attr_info.get('override_date', datetime.now().strftime('%Y-%m-%d'))
            })
            
        df = pd.DataFrame(data)
        
        # Add a comment header to the CSV file
        header_comment = "# NOMAD Sample Attribution Overrides\n# This file contains manual corrections to sample attributions.\n"
        
        # Write the header comment first
        with open(filename, 'w') as f:
            f.write(header_comment)
        
        # Then append the CSV data
        df.to_csv(filename, index=False, mode='a')
        
        print(f"Saved {len(attributions)} attribution overrides to {filename}")
        
        # If we're saving to the new filename and the old file exists, add a migration note
        old_filename = 'nomad_samples_with_authors.csv'
        if filename != old_filename and os.path.exists(old_filename):
            with open(old_filename, 'w') as f:
                f.write("# MIGRATED: Attribution data has been moved to 'attribution_overrides.csv'\n")
                f.write("# This file is kept for backwards compatibility.\n")
            print(f"Added migration note to {old_filename}")
            
        return True
        
    except Exception as e:
        print(f"Error saving attributions: {str(e)}")
        return False