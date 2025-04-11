"""
NOMAD API Data Retrieval Module

This module provides higher-level functions for retrieving and processing data from the NOMAD API.
It builds on the client.py module to provide specific data retrieval functions.
"""
from typing import Dict, List, Optional, Any

from nomad_api.client import NomadClient

def get_all_samples_with_authors(
    client: NomadClient, 
    page_size: int = 100, 
    max_pages: Optional[int] = None,
    section_type: str = "HySprint_Sample",
    show_progress: bool = True
) -> List[Dict[str, Any]]:
    """
    Retrieve all samples with their author information
    
    Args:
        client: NomadClient instance
        page_size: Number of entries per page
        max_pages: Maximum number of pages to retrieve (None for all)
        section_type: Type of section to filter by in the query
        show_progress: Whether to print progress information
        
    Returns:
        List of dictionaries with sample data and author information
    """
    samples_with_authors = []
    page = 1
    total_pages = float('inf')  # Will be updated after first query
    
    # Query for samples (entries with specified section type)
    query_payload = {
        "owner": "visible",
        "query": {
            "and": [
                {"results.eln.sections:any": [section_type]},
                {"quantities:all": ["data"]},
            ]
        },
        "pagination": {
            "page_size": page_size,
            "page": page
        }
    }
    
    if show_progress:
        print("Retrieving samples with author information...")
    
    while page <= total_pages and (max_pages is None or page <= max_pages):
        # Update pagination for the current page
        query_payload['pagination']['page'] = page
        
        # Get entries for the current page
        response_entries = client.make_request("post", "entries/query", json_data=query_payload)
        
        # Update total pages on first query
        if page == 1:
            total_entries = response_entries.get('pagination', {}).get('total', 0)
            total_pages = (total_entries + page_size - 1) // page_size
            
            if show_progress:
                print(f"Found {total_entries} samples (approximately {total_pages} pages)")
                
                if max_pages:
                    print(f"Limiting to {max_pages} pages ({min(max_pages * page_size, total_entries)} samples)")
        
        entries = response_entries.get('data', [])
        
        # Process entries in current page
        for entry in entries:
            try:
                # Extract basic sample information
                sample_info = {
                    'entry_id': entry.get('entry_id'),
                    'upload_id': entry.get('upload_id'),
                    'lab_id': entry.get('data', {}).get('lab_id')
                }
                
                # Get details about the upload (which contains author information)
                upload_id = entry.get('upload_id')
                if upload_id:
                    response_upload = client.make_request("get", f"uploads/{upload_id}")
                    upload_data = response_upload.get('data', {})
                    
                    # Add author information
                    sample_info.update({
                        'main_author': upload_data.get('main_author'),
                        'coauthors': upload_data.get('coauthors', []),
                        'coauthor_groups': upload_data.get('coauthor_groups', []),
                        'upload_create_time': upload_data.get('upload_create_time'),
                        'published': upload_data.get('published', False),
                        'license': upload_data.get('license'),
                        'upload_name': upload_data.get('upload_name'),
                    })
                
                samples_with_authors.append(sample_info)
                
            except Exception as e:
                if show_progress:
                    print(f"Error processing entry {entry.get('entry_id')}: {str(e)}")
        
        if show_progress:
            print(f"Processed page {page}/{total_pages if max_pages is None else min(max_pages, total_pages)}")
        page += 1
    
    if show_progress:
        print(f"Retrieved information for {len(samples_with_authors)} samples")
    
    return samples_with_authors


def get_user_details(client: NomadClient, user_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information about a user by their ID
    
    Args:
        client: NomadClient instance
        user_id: The ID of the user to retrieve
        
    Returns:
        User details dictionary or None if not found
    """
    try:
        response = client.make_request('get', f'users/{user_id}')
        return response
    except Exception as e:
        print(f"Error getting user {user_id}: {str(e)}")
        return None


def get_all_unique_authors(samples: List[Dict[str, Any]]) -> set:
    """Extract all unique author IDs from a list of samples
    
    Args:
        samples: List of sample dictionaries with 'main_author' and 'coauthors' keys
        
    Returns:
        Set of unique author IDs
    """
    unique_authors = set()
    
    # Add main authors
    for sample in samples:
        if 'main_author' in sample and sample['main_author']:
            unique_authors.add(sample['main_author'])
            
        # Add coauthors
        coauthors = sample.get('coauthors', [])
        if isinstance(coauthors, list):
            unique_authors.update([author for author in coauthors if author])
            
    return unique_authors


def create_author_name_map(client: NomadClient, samples: List[Dict[str, Any]]) -> Dict[str, str]:
    """Create a mapping from author ID to author name
    
    Args:
        client: NomadClient instance
        samples: List of sample dictionaries
    
    Returns:
        Dictionary mapping author IDs to author names
    """
    # Get all unique author IDs
    unique_authors = get_all_unique_authors(samples)
    
    # Create a dictionary to store user details
    user_cache = {}
    
    # Get user details for each unique author
    for user_id in unique_authors:
        if user_id and user_id not in user_cache:
            user_details = get_user_details(client, user_id)
            if user_details:
                user_cache[user_id] = user_details
    
    # Create mapping from user ID to name
    user_id_to_name = {}
    for user_id, user_info in user_cache.items():
        name = user_info.get('name') or user_info.get('username', 'Unknown')
        user_id_to_name[user_id] = name
        
    return user_id_to_name


def query_sample_entries(client: NomadClient, query: Dict[str, Any] = None, 
                        section_type: str = "HySprint_Sample", page_size: int = 100) -> Dict[str, Any]:
    """Query for sample entries based on custom criteria
    
    Args:
        client: NomadClient instance
        query: Additional query parameters to include (will be combined with section filter)
        section_type: Type of section to filter by
        page_size: Number of entries per page
        
    Returns:
        Response data from the API
    """
    # Start with basic section filter
    base_query = {
        "owner": "visible",
        "query": {
            "and": [
                {f"results.eln.sections:any": [section_type]}
            ]
        },
        "pagination": {
            "page_size": page_size,
            "page": 1
        }
    }
    
    # Add additional query parameters if provided
    if query and isinstance(query, dict):
        if 'and' in query:
            # Combine with existing AND conditions
            base_query['query']['and'].extend(query['and'])
        else:
            # Add as additional AND condition
            for k, v in query.items():
                if k != 'owner' and k != 'pagination':  # Don't override these
                    base_query['query']['and'].append({k: v})
    
    # Execute the query
    response = client.make_request("post", "entries/query", json_data=base_query)
    return response