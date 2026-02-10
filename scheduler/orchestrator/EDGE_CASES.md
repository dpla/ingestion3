# Notification System - Edge Cases and Known Limitations

## Fixed Issues
- **Errors/Warnings regex** - Fixed to match `- Errors...` format in `_SUMMARY` files

## Current Limitations

### 1. Start Notification Not Tested in Dry-Run
- **Issue**: `--dry-run-notify` tests completion and anomaly alerts, but not `send_start_notification`
- **Risk**: Low - start notification is simpler and unlikely to fail
- **Mitigation**: Can manually test with a real ingest dry-run

### 2. Draft Directory Path Shows Local Path
- **Issue**: Slack message shows `Draft emails: /Users/scott/...` which isn't useful for remote teams
- **Risk**: Medium - team members can't access local files
- **Recommendation**: Consider uploading drafts to S3 or shared location

### 3. Email Drafts Don't Include Attachments
- **Issue**: Original `Emailer.scala` attaches `logs.zip`, but draft files don't
- **Risk**: Low - drafts are for proofreading, not final sending
- **Mitigation**: Include S3 links to logs in the draft body (already implemented)

### 4. Presigned URLs May Fail Silently
- **Issue**: If boto3 and AWS CLI both fail, URLs fall back to console URLs
- **Risk**: Low - console URLs still work, just require AWS login
- **Mitigation**: Warning printed to console on failure

### 5. No Verification of Slack Delivery
- **Issue**: We send to webhook but don't verify message was delivered
- **Risk**: Low - Slack webhooks are reliable, return 200 on success
- **Mitigation**: Check response code (currently not logged)

### 6. Skipped Hubs Not Shown in Notification
- **Issue**: Hubs with status='skipped' aren't listed in completion message
- **Risk**: Low - skipped hubs are rare edge cases
- **Recommendation**: Add "Skipped:" section if needed

### 7. Enrichment Summary vs Mapping Summary
- **Issue**: `_SUMMARY` exists in both `mapping/` and `enrichment/` dirs with different formats
- **Current Behavior**: Code reads from `mapping/` which has correct fields
- **Risk**: None - enrichment summary isn't needed for notifications

### 8. Large Hub Lists May Truncate Important Info
- **Issue**: With many hubs, message truncates and may cut off failure details
- **Risk**: Medium - could miss important failures
- **Recommendation**: Prioritize failures at top of message

### 9. Hub Names with Special Characters
- **Issue**: Hub names like `northwest-heritage` work fine, but edge cases untested
- **Risk**: Low - all known hubs have alphanumeric + hyphen names

### 10. Data Directory Missing or Inaccessible
- **Issue**: If `config.data_dir` doesn't exist, enrichment will fail silently
- **Risk**: Low - unlikely in production
- **Mitigation**: Returns empty counts, notification still sent

## Test Coverage

### Covered by `--dry-run-notify`
- Completion notification with real hub data
- Warning-level anomaly alerts
- Critical-level anomaly alerts
- Email draft generation
- Slack message formatting
- "No email" hub detection

### Covered by `pytest` (unit tests)
- Message formatting functions
- Hub count parsing
- Failure stage inference
- Message truncation

### Not Covered (requires integration test)
- Real S3 presigned URL generation
- Real ingest end-to-end
- Webhook failure handling
- Concurrent notification sending

## Recommendations for Production

1. **Set `SLACK_WEBHOOK` persistently**:
   ```bash
   # Add to ~/.bashrc or ~/.zshrc
   export SLACK_WEBHOOK="https://hooks.slack.com/services/..."
   ```

2. **Or use a virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install python-dotenv boto3
   ```

3. **Test with single hub first**:
   ```bash
   python3 -m scheduler.orchestrator.main --hub=maryland --parallel=1
   ```

4. **Monitor #tech-alerts during first production run**
