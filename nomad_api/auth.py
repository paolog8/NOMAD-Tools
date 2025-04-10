"""
NOMAD API Authentication Module

This module handles authentication with NOMAD API endpoints.
"""
import os
import requests
import json
from typing import Dict, Optional, Union, Any, List, Tuple


# Dictionary mapping user-friendly names to actual API URLs
OASIS_OPTIONS = {
    "SE Oasis": "https://nomad-hzb-se.de/nomad-oasis/api/v1",
    "CE Oasis": "https://nomad-hzb-ce.de/nomad-oasis/api/v1",
    "Sol-AI Oasis": "https://nomad-sol-ai.de/nomad-oasis/api/v1",
}

def get_token(url: str, username: str, password: str) -> str:
    """
    Get authentication token from the NOMAD API using username and password.
    
    Args:
        url: The base URL for the NOMAD API
        username: NOMAD username
        password: NOMAD password
        
    Returns:
        The authentication token as a string
        
    Raises:
        ValueError: If authentication fails
    """
    try:
        response = requests.get(
            f"{url}/auth/token", params=dict(username=username, password=password))
        response.raise_for_status()
        
        token_data = response.json()
        if 'access_token' not in token_data:
            raise ValueError("Access token not found in response")
            
        return token_data['access_token']
    except requests.exceptions.RequestException as e:
        error_message = f"Authentication failed: {e}"
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json().get('detail', e.response.text)
                if isinstance(error_detail, list):
                    error_message = f"API Error ({e.response.status_code}): {json.dumps(error_detail)}"
                else:
                    error_message = f"API Error ({e.response.status_code}): {error_detail}"
            except json.JSONDecodeError:
                error_message = f"API Error ({e.response.status_code}): {e.response.text}"
        raise ValueError(error_message) from e


def get_token_from_env() -> str:
    """
    Get authentication token from environment variable.
    
    Returns:
        The authentication token as a string
        
    Raises:
        ValueError: If token is not found in environment
    """
    token = os.environ.get('NOMAD_CLIENT_ACCESS_TOKEN')
    if not token:
        raise ValueError("Token not found in environment variable 'NOMAD_CLIENT_ACCESS_TOKEN'")
    return token


def verify_token(base_url: str, token: str) -> Dict[str, Any]:
    """
    Verify if a token is valid by making a request to the users/me endpoint.
    
    Args:
        base_url: The base URL for the NOMAD API
        token: Authentication token to verify
        
    Returns:
        User info dictionary if token is valid
        
    Raises:
        ValueError: If token verification fails
    """
    try:
        verify_url = f"{base_url}/users/me"
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(verify_url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_message = f"Token verification failed: {e}"
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json().get('detail', e.response.text)
                error_message = f"API Error ({e.response.status_code}): {error_detail}"
            except json.JSONDecodeError:
                error_message = f"API Error ({e.response.status_code}): {e.response.text}"
        raise ValueError(error_message) from e


def authenticate(base_url: str, method: str = "token", username: str = None, 
                password: str = None) -> Tuple[str, Dict[str, Any]]:
    """
    Authenticate with the NOMAD API using either username/password or token from environment.
    
    Args:
        base_url: The base URL for the NOMAD API
        method: Authentication method, either "password" or "token"
        username: Username for password authentication
        password: Password for password authentication
        
    Returns:
        Tuple of (token, user_info)
        
    Raises:
        ValueError: If authentication fails
    """
    if method == "password":
        if not username or not password:
            raise ValueError("Username and password are required for password authentication")
        token = get_token(base_url, username, password)
    elif method == "token":
        token = get_token_from_env()
    else:
        raise ValueError(f"Unsupported authentication method: {method}")
        
    # Verify the token and get user info
    user_info = verify_token(base_url, token)
    
    return token, user_info