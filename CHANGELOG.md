# NOMAD-Tools Changelog

All notable changes to the NOMAD-Tools repository will be documented in this file.

## 2025-04-10

### Added
- Initial repository setup with Git integration
- NOMAD_Group_Management_Dashboard.ipynb for managing NOMAD groups, users, and permissions
  - Authentication with multiple NOMAD Oasis instances
  - Group visualization features to view current members
  - Add members to groups via email addresses
  - Remove members from groups with multi-select interface
  - Create new groups with optional initial members
  - Delete groups with name confirmation for safety
  - Environment variable support for credentials
- api_calls.py with functions for interacting with the NOMAD API
- get_all_uploads_of_author.ipynb for retrieving uploads by a specific author
- Created initial package structure for future modular code organization

### Changed
- Organized repository with basic file structure
- Set up remote GitHub connection
- Initialized Git repository

### Fixed
- Fixed Git repository configuration issues