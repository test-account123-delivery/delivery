# Updated old_working_code.py with Email Flow

This document describes the updates made to `old_working_code.py` to incorporate the email flow functionality from the reference code.

## Changes Made

### 1. Updated Imports
- Added email_validator, yaml, jinja2, oracledb, zoneinfo imports
- Added dataclass, datetime, enum, typing imports for better type hints

### 2. New Classes and Enums
- **AppWorxEnum**: Centralized enum for all AppWorx arguments to avoid hard-coded strings
- **ScriptData**: Dataclass to hold all script structures and data (apwx, dbh, config, email_template)

### 3. Enhanced Email Flow Functions
- **validate_email()**: Validates email addresses using email_validator
- **send_email()**: New comprehensive email sending function
- **generate_email_message()**: Creates EmailMessage objects with proper headers
- **generate_email_content()**: Renders Jinja2 templates with account data
- **send_smtp_request()**: Handles SMTP connection and authentication
- **is_local_environment()**: Checks if running in local development environment
- **send_email_enabled()**: Checks if email sending is enabled

### 4. Configuration and Template Support
- **get_config()**: Loads YAML configuration files
- **get_email_template()**: Loads Jinja2 email templates
- **execute_sql_select()**: Enhanced database query execution with proper error handling

### 5. New Email Processing Functions
- **get_closed_accounts()**: Retrieves closed accounts based on configuration
- **process_records()**: Processes accounts and sends emails with validation
- **write_audit_log()**: Generates comprehensive audit reports
- **write_csv()**: Helper function for CSV output

### 6. Utility Functions
- **today_date()**: Returns current date in Los Angeles timezone
- **is_fdi()**: Checks for existing active 8FDI notes
- **format_minor_codes()**: Formats minor codes for SQL IN clauses

## Usage

### Legacy Mode (Backward Compatibility)
The script maintains backward compatibility. If no `CONFIG_FILE_PATH` is provided, it will use the original functionality:

```bash
python old_working_code.py [original parameters]
```

### New Email Flow Mode
To use the new email flow functionality, provide the `CONFIG_FILE_PATH` parameter:

```bash
python old_working_code.py \
  --TNS_SERVICE_NAME=your_service \
  --CONFIG_FILE_PATH=sample_config.yaml \
  --EFFDATE=01-01-2024 \
  --OUTPUT_FILE_PATH=/path/to/output \
  --OUTPUT_FILE_NAME=report.csv \
  --SMTP_SERVER=smtp.example.com \
  --SMTP_PORT=587 \
  --SMTP_USER=user@example.com \
  --SMTP_PASSWORD=password \
  --SEND_EMAIL_YN=Y
```

## New Parameters

The following new parameters are available for the email flow:

- **CONFIG_FILE_PATH**: Path to YAML configuration file (required for new flow)
- **EFFDATE**: Effective date for closed account processing
- **FROM_EMAIL_ADDR**: From email address (default: member.communications@firsttechfed.com)
- **MINOR_CODES**: Comma-separated list of minor codes (default: NACL,NAIL,UAOE,etc.)
- **SMTP_PORT**: SMTP server port
- **SMTP_USER**: SMTP authentication username
- **SMTP_PASSWORD**: SMTP authentication password
- **TEST_EMAIL_ADDR**: Optional test email address to override recipients

## Configuration File

The script expects a YAML configuration file (see `sample_config.yaml`) with:

- **template_directory**: Directory containing email templates
- **template_file**: Name of the email template file
- **csv_header**: List of CSV column headers
- **get_closed_accounts**: SQL query for retrieving closed accounts

## Email Templates

Email templates should be Jinja2 HTML templates placed in the configured template directory. Available variables:

- `{{ membername }}`: Member's full name
- `{{ emaildate }}`: Email/close date
- `{{ year }}`: Current year

## Error Handling

The updated code includes comprehensive error handling:

- Email validation before sending
- SMTP connection error handling
- Database query error handling
- Configuration file validation
- Template loading error handling

## Dependencies

Make sure the following Python packages are installed:

```bash
pip install email-validator PyYAML Jinja2 oracledb
```

## Files Created

- `sample_config.yaml`: Example configuration file
- `templates/closed_account_email.html`: Sample email template
- `README.md`: This documentation file

The script now supports both the original functionality and the new email flow, making it flexible for different use cases while maintaining backward compatibility.