"""
NOMAD API Client Module

This module provides a client for interacting with NOMAD API endpoints.
"""
import requests
import json
from typing import Dict, Optional, Union, Any, List, Tuple, Callable


class NomadClient:
    """Client for interacting with the NOMAD API."""
    
    def __init__(self, base_url: str, token: str):
        """
        Initialize the NOMAD API client.
        
        Args:
            base_url: The base URL for the NOMAD API
            token: Authentication token
        """
        self.base_url = base_url
        self.token = token
        self.headers = {'Authorization': f'Bearer {token}'}
        
    def make_request(self, method: str, endpoint: str, params: Dict = None, 
                    json_data: Dict = None, timeout: int = 10) -> Any:
        """
        Make an API request to a NOMAD endpoint.
        
        Args:
            method: HTTP method (get, post, delete, etc.)
            endpoint: API endpoint path (without base_url)
            params: Query parameters
            json_data: JSON payload for POST/PUT requests
            timeout: Request timeout in seconds
            
        Returns:
            Response data as dictionary or None
            
        Raises:
            ConnectionError: If the request fails
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.request(
                method, 
                url, 
                headers=self.headers, 
                params=params, 
                json=json_data, 
                timeout=timeout
            )
            response.raise_for_status()
            
            # Return JSON data if there is a response body, otherwise None
            if response.text:
                return response.json()
            return None
            
        except requests.exceptions.RequestException as e:
            error_message = f"API request failed: {e}"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json().get('detail', e.response.text)
                    if isinstance(error_detail, list):
                        error_message = f"API Error ({e.response.status_code}): {json.dumps(error_detail)}"
                    else:
                        error_message = f"API Error ({e.response.status_code}): {error_detail or e.response.text}"
                except json.JSONDecodeError:
                    error_message = f"API Error ({e.response.status_code}): {e.response.text}"
            raise ConnectionError(error_message) from e
        except Exception as e:
            raise Exception(f"Unexpected error during API request: {e}") from e
    
    # User-related methods
    def get_user_info(self) -> Dict[str, Any]:
        """Get information about the authenticated user."""
        return self.make_request('get', 'users/me')
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user information by email address."""
        response = self.make_request('get', 'users', params={'email': email})
        if response and 'data' in response and len(response['data']) > 0:
            return response['data'][0]
        return None
    
    # Group-related methods
    def get_groups(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        """Get all groups accessible to the authenticated user."""
        response = self.make_request('get', 'groups', params={'page_size': page_size})
        return response.get('data', []) if response else []
        
    def get_group_details(self, group_id: str) -> Dict[str, Any]:
        """Get details of a specific group."""
        return self.make_request('get', f'groups/{group_id}')
        
    def create_group(self, group_name: str, members: List[str] = None) -> Dict[str, Any]:
        """
        Create a new group.
        
        Args:
            group_name: Name for the new group
            members: List of user IDs to add to the group (optional)
            
        Returns:
            The created group data
        """
        payload = {'group_name': group_name}
        if members:
            payload['members'] = members
        return self.make_request('post', 'groups', json_data=payload)
        
    def update_group_members(self, group_id: str, members: List[str]) -> Dict[str, Any]:
        """
        Update the members of a group.
        
        Args:
            group_id: ID of the group to update
            members: Complete list of user IDs that should be members
            
        Returns:
            Updated group data
        """
        return self.make_request('post', f'groups/{group_id}/edit', json_data={'members': members})
        
    def delete_group(self, group_id: str) -> None:
        """Delete a group."""
        self.make_request('delete', f'groups/{group_id}')
    
    # Entry-related methods
    def query_entries(self, query: Dict, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        Query entries in NOMAD with advanced filtering.
        
        Args:
            query: Query filter dictionary
            page_size: Number of results per page
            
        Returns:
            List of matching entries
        """
        if 'pagination' not in query:
            query['pagination'] = {'page_size': page_size}
            
        response = self.make_request('post', 'entries/archive/query', json_data=query)
        return response.get('data', []) if response else []