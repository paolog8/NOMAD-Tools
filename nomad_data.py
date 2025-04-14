"""
NOMAD Data Retrieval Module

This module provides functions for retrieving and processing data from NOMAD,
specifically focused on HySprint samples.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from tqdm.notebook import tqdm  # For nice progress bars in notebooks

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
    Retrieve HySprint sample data from NOMAD
    
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
        print(f"Retrieving {'all' if max_entries is None else f'up to {max_entries}'} HySprint samples...")
        
        # Define the query payload for HySprint samples
        query_payload = {
            "owner": "visible",
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
        
        # Get first page to determine total entries
        response = client.make_request("post", "entries/query", json_data=query_payload)
        
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
                upload_response = client.make_request("get", f"uploads/{upload_id}")
                upload_data = upload_response.get('data', {})
                
                # Get upload name
                upload_name = upload_data.get('upload_name', '')
                
                # Get author details
                author_id = upload_data.get('main_author', '')
                if author_id and author_id not in user_cache:
                    user_cache[author_id] = get_user_details(client, author_id)
                
                author_info = user_cache.get(author_id, {})
                author_name = author_info.get('name', author_info.get('username', author_id))
                
                # Extract sample data
                sample_info = {
                    'upload_id': upload_id,
                    'upload_name': upload_name,  # Added upload name here
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
            
        # Load attributions from CSV
        df = pd.read_csv(filename)
        
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