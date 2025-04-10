"""
NOMAD API Convenience Functions

This module provides higher-level functions for common NOMAD API operations,
built on top of the nomad_api package.
"""

import os
from typing import List, Dict, Any, Optional

from nomad_api.auth import get_token_from_env, OASIS_OPTIONS
from nomad_api.client import NomadClient


def get_client(url: str = None, token: str = None, oasis_name: str = None) -> NomadClient:
    """
    Get a configured NomadClient instance.
    
    Uses the following precedence for URL:
    1. Explicit url parameter
    2. URL from oasis_name lookup
    3. SE Oasis as default
    
    Uses the following precedence for token:
    1. Explicit token parameter
    2. Environment variable NOMAD_CLIENT_ACCESS_TOKEN
    
    Args:
        url: Optional explicit API URL
        token: Optional explicit token
        oasis_name: Optional name of oasis instance to use (from OASIS_OPTIONS)
        
    Returns:
        Configured NomadClient instance
    """
    # Determine URL
    if not url:
        if oasis_name and oasis_name in OASIS_OPTIONS:
            url = OASIS_OPTIONS[oasis_name]
        else:
            url = OASIS_OPTIONS["SE Oasis"]  # Default
    
    # Get token
    if not token:
        token = get_token_from_env()
        
    return NomadClient(url, token)


def get_batch_ids(client: Optional[NomadClient] = None, 
                 url: str = None, token: str = None, 
                 batch_type: str = "HySprint_Batch") -> List[str]:
    """
    Get all batch IDs of a specified type.
    
    Args:
        client: Optional NomadClient instance
        url: API URL (used if client not provided)
        token: Auth token (used if client not provided)
        batch_type: Type of batch to query
        
    Returns:
        List of batch IDs
    """
    if client is None:
        client = get_client(url, token)
    
    query = {
        'required': {'data': '*'},
        'owner': 'visible',
        'query': {'entry_type': batch_type},
        'pagination': {'page_size': 10000}
    }
    
    data = client.query_entries(query)
    return [d["archive"]["data"]["lab_id"] for d in data if "lab_id" in d["archive"]["data"]]


def get_ids_in_batch(client: Optional[NomadClient] = None, 
                    url: str = None, token: str = None,
                    batch_ids: List[str] = None, 
                    batch_type: str = "HySprint_Batch") -> List[str]:
    """
    Get all entity IDs in specified batches.
    
    Args:
        client: Optional NomadClient instance
        url: API URL (used if client not provided)
        token: Auth token (used if client not provided)
        batch_ids: List of batch IDs to query
        batch_type: Type of batch to query
        
    Returns:
        List of entity IDs in the batches
    """
    if not batch_ids:
        return []
        
    if client is None:
        client = get_client(url, token)
    
    query = {
        'required': {'data': '*'},
        'owner': 'visible',
        'query': {'results.eln.lab_ids:any': batch_ids, 'entry_type': batch_type},
        'pagination': {'page_size': 100}
    }
    
    data = client.query_entries(query)
    sample_ids = []
    for d in data:
        dd = d["archive"]["data"]
        if "entities" in dd:
            sample_ids.extend([s["lab_id"] for s in dd["entities"]])
    return sample_ids


def get_uploads_by_author(client: Optional[NomadClient] = None,
                         url: str = None, token: str = None,
                         author: str = None) -> List[str]:
    """
    Get all uploads by a specific author.
    
    Args:
        client: Optional NomadClient instance
        url: API URL (used if client not provided)
        token: Auth token (used if client not provided)
        author: Author name to search for
        
    Returns:
        List of upload IDs by the author
    """
    if not author:
        return []
        
    if client is None:
        client = get_client(url, token)
    
    query = {
        'required': {'data': '*'},
        'owner': 'visible',
        'query': {'authors': author},
        'pagination': {'page_size': 10000}
    }
    
    data = client.query_entries(query)
    return [d["archive"]["data"]["lab_id"] for d in data if "lab_id" in d["archive"]["data"]]
