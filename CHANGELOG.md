# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-01

### Added
- Initial Python implementation of feishu-bitable-db
- Complete translation from Go to Python
- Database operations: create, list, drop
- Table operations: create/update, list, drop
- Record operations: CRUD (Create, Read, Update, Delete)
- Field type support: String, Int, Radio, MultiSelect, Date, People
- Comprehensive test suite
- Examples for basic and advanced usage
- Full project documentation

### Changed
- Converted from Go naming conventions to Python (snake_case)
- Improved error handling with Python exceptions
- Used Python type hints throughout the codebase
- Implemented with dataclasses and enums for better type safety

### Technical Details
- Based on lark-oapi SDK for Feishu API integration
- Uses loguru for logging instead of logrus
- Implements abstract base classes for better interface definition
- Includes development tools configuration (black, isort, flake8, mypy)