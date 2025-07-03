# Email Notifications System for SoftCollection

## Overview

The SoftCollection system includes an automated email notification system that sends notifications when campaigns are successfully created or when errors occur during the stratification process.

## Features

- **Success Notifications**: Detailed emails when campaigns are successfully created and loaded into databases
- **Error Notifications**: Alert emails when campaign creation fails
- **Dual Database Status**: Reports on both DSSB_APP and SPSS database insertion results
- **Rich HTML Formatting**: Professional-looking emails with tables and formatting
- **JSON Attachments**: Detailed campaign data attached to success emails
- **Automatic Detection**: No manual intervention required - emails sent automatically

## Email Types

### 1. Campaign Success Notification

**Sent when**: All campaign groups are successfully created and loaded into databases

**Content includes**:
- Campaign summary with base ID and total users
- Group-by-group breakdown (A, B, C, D, E)
- Database insertion status (DSSB_APP + SPSS)
- Execution time and performance metrics
- Target table information (SC_local_control vs SC_local_target)
- JSON attachment with complete campaign details

**Example Subject**: `✅ SoftCollection: Кампания SC00000001 успешно создана`

### 2. Campaign Error Notification

**Sent when**: Campaign creation fails at any stage

**Content includes**:
- Error details and stack traces
- Failed operation information
- User who attempted the operation
- Recommended troubleshooting steps
- Timestamp of failure

**Example Subject**: `❌ SoftCollection: Ошибка создания кампании`

## Configuration

### Default Recipients (Hardcoded)

The system comes with hardcoded email addresses that you should replace:

```python
CAMPAIGN_NOTIFICATION_EMAILS = [
    'manager@example.com',   # Replace with actual manager email
    'admin@example.com',     # Replace with actual admin email  
    'analyst@example.com'    # Replace with actual analyst email
]
```

### Environment Variable Override

You can override the default recipients using environment variables:

```bash
# Comma-separated list of email addresses
CAMPAIGN_NOTIFICATION_EMAILS=manager@company.com,admin@company.com,analyst@company.com
```

### SMTP Configuration

The system includes hardcoded SMTP credentials for immediate use:

```python
EMAIL_SENDER = 'cdsrb@halykbank.kz'
SMTP_SERVER = 'mail.halykbank.nb'
SMTP_PORT = 587
SMTP_USERNAME = 'cdsrb'
SMTP_PASSWORD = 'oREgNwtWr9B5F4dsGjjZ'
```

You can override these with environment variables:

```bash
EMAIL_SENDER=your-sender@company.com
SMTP_SERVER=mail.company.com
SMTP_PORT=587
SMTP_USERNAME=your-username
SMTP_PASSWORD=your-password
```

## Email Flow During Stratification

```
User Initiates Stratification
├── Query Execution
├── Data Stratification (3-5 groups)
├── Theory Creation
├── Database Insertion
│   ├── Control Group → DSSB_APP.SC_local_control
│   └── Target Groups → DSSB_APP.SC_local_target + SPSS.SC_theory_users
├── Success? 
│   ├── YES → Send Success Email ✅
│   └── NO → Send Error Email ❌
└── Return API Response
```

## Testing Email Notifications

### API Endpoints for Testing

1. **Test Email Configuration**:
   ```bash
   GET /test/email-config
   ```
   Returns current email configuration (without sensitive data)

2. **Send Test Email**:
   ```bash
   POST /test/email-notifications
   ```
   Sends a test campaign success email

3. **Check Email Dependencies**:
   ```bash
   GET /test/stratification-deps
   ```
   Includes email module availability in dependency check

### Manual Testing

You can test the email sender directly:

```bash
cd database-backend
python email_sender.py
```

This will:
- Validate email configuration
- Send a test email to configured recipients
- Display success/failure status

## Customizing Email Templates

### Success Email Template

The success email includes:
- **Header**: Gradient background with campaign status
- **Summary Table**: Campaign ID, user count, execution time, database status
- **Groups Table**: Detailed breakdown of each group (A, B, C, D, E)
- **Database Info**: Explanation of where data is stored
- **Footer**: Timestamp and system information

### Error Email Template

The error email includes:
- **Header**: Red gradient background indicating error
- **Error Details**: Operation, user, timestamp, error message
- **Troubleshooting**: Recommended actions to resolve issues
- **Contact Info**: System information for support

### Customization

To customize email templates, modify the functions in `email_sender.py`:

- `create_campaign_success_email()` - Success notification template
- `create_campaign_error_email()` - Error notification template

## Security Considerations

1. **Hardcoded Credentials**: The system includes hardcoded SMTP credentials for immediate functionality. In production:
   - Use environment variables
   - Consider using encrypted credential storage
   - Regularly rotate passwords

2. **Email Content**: Emails may contain sensitive campaign information:
   - Ensure recipient list is appropriate
   - Consider email encryption for sensitive data
   - Monitor email logs for delivery issues

3. **Attachment Security**: JSON attachments contain detailed campaign data:
   - Ensure secure email transmission
   - Consider data classification policies
   - Limit attachment size if needed

## Troubleshooting

### Common Issues

1. **Emails Not Sending**
   - Check SMTP server connectivity
   - Verify credentials and authentication
   - Check firewall and network settings
   - Review application logs for SMTP errors

2. **Wrong Recipients**
   - Update hardcoded recipients in `email_sender.py`
   - Set `CAMPAIGN_NOTIFICATION_EMAILS` environment variable
   - Test with `/test/email-config` endpoint

3. **Email Formatting Issues**
   - Check HTML rendering in different email clients
   - Test with various email providers
   - Verify encoding and character set handling

4. **Large Attachments**
   - Monitor JSON attachment sizes for large campaigns
   - Consider compression for very large datasets
   - Check email server attachment limits

### Logging

Email sending is logged with the following information:
- Success/failure status
- Recipient lists
- Error messages and stack traces
- Timestamp and operation details

Check logs at: `email_sender.log` (configurable via `EMAIL_LOG_FILE` environment variable)

## Integration with Existing Systems

The email notification system is designed to:
- **Not interfere** with core functionality (emails are sent asynchronously)
- **Fail gracefully** if email sending fails (campaign creation still succeeds)
- **Provide detailed feedback** for monitoring and auditing
- **Support multiple notification channels** (can be extended for Slack, Teams, etc.)

## Future Enhancements

Potential improvements for the email system:
- **Email Templates**: Configurable templates via database or files
- **Multiple Channels**: Slack, Microsoft Teams, SMS notifications
- **Conditional Notifications**: Send emails only for certain campaign types or sizes
- **Email Scheduling**: Batch notifications or scheduled summaries
- **Rich Attachments**: Excel reports, charts, dashboard links 