"""Error classification and diagnosis for ingest failures."""

import re
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


class ErrorType(str, Enum):
    """Types of errors that can occur during ingestion."""
    OOM = "out_of_memory"
    TIMEOUT = "timeout"
    NO_OUTPUT = "no_output"
    SBT_CONFLICT = "sbt_conflict"
    S3_ACCESS = "s3_access"
    OAI_FEED = "oai_feed_error"
    PREPROCESSING = "preprocessing"
    CONFIG = "configuration"
    NETWORK = "network"
    UNKNOWN = "unknown"


@dataclass
class Diagnosis:
    """Diagnosis for an error with suggested fixes."""
    error_type: ErrorType
    description: str
    suggested_fix: str
    can_auto_retry: bool
    needs_preprocessing: bool = False
    needs_human: bool = False
    context: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'error_type': self.error_type.value,
            'description': self.description,
            'suggested_fix': self.suggested_fix,
            'can_auto_retry': self.can_auto_retry,
            'needs_preprocessing': self.needs_preprocessing,
            'needs_human': self.needs_human,
            'context': self.context,
        }


class ErrorClassifier:
    """Classify errors and generate diagnoses."""
    
    # Error patterns to match against logs
    PATTERNS = {
        ErrorType.OOM: [
            r"OutOfMemoryError",
            r"Java heap space",
            r"GC overhead limit exceeded",
            r"Required array length.*is too large",
        ],
        ErrorType.TIMEOUT: [
            r"Connection.*timed out",
            r"SocketTimeoutException",
            r"Read timed out",
            r"connect timed out",
            r"HttpTimeoutException",
        ],
        ErrorType.SBT_CONFLICT: [
            r"Address already in use",
            r"sbt.*already booting",
            r"ServerAlreadyBootingException",
            r"waiting for lock on.*sbt\.boot\.lock",
        ],
        ErrorType.NO_OUTPUT: [
            r"No harvest data found",
            r"NullPointerException.*InputHelper",
            r"mostRecentLocal.*null",
        ],
        ErrorType.S3_ACCESS: [
            r"Access Denied",
            r"NoSuchBucket",
            r"InvalidAccessKeyId",
            r"SignatureDoesNotMatch",
            r"bucket.*does not exist",
        ],
        ErrorType.OAI_FEED: [
            r"cannotDisseminateFormat",
            r"noRecordsMatch",
            r"badVerb",
            r"idDoesNotExist",
            r"OAI.*error",
        ],
        ErrorType.NETWORK: [
            r"UnknownHostException",
            r"ConnectException",
            r"NoRouteToHostException",
            r"Connection refused",
        ],
        ErrorType.CONFIG: [
            r"Configuration.*not found",
            r"Missing required",
            r"Invalid endpoint",
        ],
    }
    
    # Suggested fixes for each error type
    FIXES = {
        ErrorType.OOM: Diagnosis(
            error_type=ErrorType.OOM,
            description="Java ran out of memory processing large files",
            suggested_fix="For Smithsonian: run xmll preprocessing to shred large XML files. "
                         "For others: increase JVM heap size in SBT_OPTS",
            can_auto_retry=False,
            needs_preprocessing=True,
            needs_human=True,
        ),
        ErrorType.TIMEOUT: Diagnosis(
            error_type=ErrorType.TIMEOUT,
            description="OAI feed or API request timed out",
            suggested_fix="Retry with exponential backoff. If persistent, check feed status "
                         "or contact provider",
            can_auto_retry=True,
            needs_human=False,
        ),
        ErrorType.SBT_CONFLICT: Diagnosis(
            error_type=ErrorType.SBT_CONFLICT,
            description="Another sbt process is using the same port/socket",
            suggested_fix="Wait for other sbt processes to complete, or kill stale processes. "
                         "Check: ps aux | grep sbt",
            can_auto_retry=True,
            needs_human=False,
        ),
        ErrorType.NO_OUTPUT: Diagnosis(
            error_type=ErrorType.NO_OUTPUT,
            description="No harvest data found - harvest may not have run or produced output",
            suggested_fix="Run harvest first, or check if OAI feed is returning records",
            can_auto_retry=False,
            needs_human=True,
        ),
        ErrorType.S3_ACCESS: Diagnosis(
            error_type=ErrorType.S3_ACCESS,
            description="Cannot access S3 bucket - permissions or credentials issue",
            suggested_fix="Check AWS credentials and bucket permissions. "
                         "Verify bucket name and --profile setting",
            can_auto_retry=False,
            needs_human=True,
        ),
        ErrorType.OAI_FEED: Diagnosis(
            error_type=ErrorType.OAI_FEED,
            description="OAI feed returned an error or no records",
            suggested_fix="Check OAI endpoint URL in i3.conf. Test feed manually with curl. "
                         "May need to contact provider if feed is down",
            can_auto_retry=False,
            needs_human=True,
        ),
        ErrorType.NETWORK: Diagnosis(
            error_type=ErrorType.NETWORK,
            description="Network connectivity issue",
            suggested_fix="Check network connection and DNS. Verify endpoint URL is correct",
            can_auto_retry=True,
            needs_human=False,
        ),
        ErrorType.CONFIG: Diagnosis(
            error_type=ErrorType.CONFIG,
            description="Configuration issue in i3.conf",
            suggested_fix="Review hub configuration in i3.conf. Check endpoint URLs and settings",
            can_auto_retry=False,
            needs_human=True,
        ),
        ErrorType.UNKNOWN: Diagnosis(
            error_type=ErrorType.UNKNOWN,
            description="Unclassified error - needs manual investigation",
            suggested_fix="Review full logs and debug manually. Check recent changes to code or config",
            can_auto_retry=False,
            needs_human=True,
        ),
    }
    
    def classify(
        self,
        hub: str,
        error: Exception,
        logs: str,
        harvest_type: str = "oai"
    ) -> Diagnosis:
        """Classify an error and return diagnosis with suggested fix."""
        error_str = f"{type(error).__name__}: {str(error)}\n{logs}"
        
        # Check each error type's patterns
        for error_type, patterns in self.PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, error_str, re.IGNORECASE):
                    diagnosis = self._create_diagnosis(error_type, hub, error, logs, harvest_type)
                    return diagnosis
        
        # No pattern matched - unknown error
        return self._create_diagnosis(ErrorType.UNKNOWN, hub, error, logs, harvest_type)
    
    def _create_diagnosis(
        self,
        error_type: ErrorType,
        hub: str,
        error: Exception,
        logs: str,
        harvest_type: str
    ) -> Diagnosis:
        """Create a diagnosis with context."""
        base = self.FIXES.get(error_type, self.FIXES[ErrorType.UNKNOWN])
        
        # Customize based on hub
        description = base.description
        suggested_fix = base.suggested_fix
        needs_preprocessing = base.needs_preprocessing
        
        if error_type == ErrorType.OOM and hub == "smithsonian":
            suggested_fix = (
                "Smithsonian files need xmll preprocessing:\n"
                "1. Download data from s3://dpla-hub-si/\n"
                "2. Recompress: gunzip -dck file.gz | gzip > fixed/file.gz\n"
                "3. Run xmll: java -Xmx4g -jar ~/xmll-assembly-0.1.jar doc fixed/file.gz xmll/file.gz\n"
                "4. Update i3.conf endpoint to point to xmll/ directory\n"
                "5. Retry harvest"
            )
            needs_preprocessing = True
        
        # Extract relevant log snippet
        log_snippet = self._extract_log_snippet(logs, error_type)
        
        return Diagnosis(
            error_type=error_type,
            description=description,
            suggested_fix=suggested_fix,
            can_auto_retry=base.can_auto_retry,
            needs_preprocessing=needs_preprocessing,
            needs_human=base.needs_human,
            context={
                'hub': hub,
                'harvest_type': harvest_type,
                'error_class': type(error).__name__,
                'error_message': str(error),
                'log_snippet': log_snippet,
            }
        )
    
    def _extract_log_snippet(self, logs: str, error_type: ErrorType) -> str:
        """Extract relevant portion of logs around the error."""
        if not logs:
            return ""
        
        # Look for error-related lines
        lines = logs.split('\n')
        relevant_lines = []
        
        keywords = ['error', 'exception', 'failed', 'fatal']
        if error_type == ErrorType.OOM:
            keywords.extend(['memory', 'heap', 'gc'])
        elif error_type == ErrorType.TIMEOUT:
            keywords.extend(['timeout', 'timed out'])
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if any(kw in line_lower for kw in keywords):
                # Include context around the error
                start = max(0, i - 2)
                end = min(len(lines), i + 3)
                relevant_lines.extend(lines[start:end])
                relevant_lines.append('---')
        
        if relevant_lines:
            return '\n'.join(relevant_lines[-50:])  # Last 50 relevant lines
        
        # No specific error lines found, return last 30 lines
        return '\n'.join(lines[-30:])
    
    def classify_from_exit_code(
        self,
        hub: str,
        exit_code: int,
        has_output: bool,
        logs: str
    ) -> Optional[Diagnosis]:
        """Classify based on exit code and output presence."""
        
        if exit_code == 0 and has_output:
            return None  # Success
        
        if exit_code == 0 and not has_output:
            # Completed but no output - likely OAI feed issue
            return Diagnosis(
                error_type=ErrorType.OAI_FEED,
                description="Harvest completed but produced no output",
                suggested_fix="OAI feed may be returning no records. Check feed URL and test manually",
                can_auto_retry=False,
                needs_human=True,
                context={'hub': hub, 'exit_code': exit_code, 'log_snippet': logs[-2000:]}
            )
        
        if exit_code == 1 and has_output:
            # Exit 1 but has output - likely minor errors, still successful
            return None
        
        if exit_code == 2:
            # Often indicates sbt conflict or missing file
            return Diagnosis(
                error_type=ErrorType.SBT_CONFLICT,
                description="sbt process conflict or missing dependency",
                suggested_fix="Wait for other sbt processes and retry. Check for stale locks",
                can_auto_retry=True,
                needs_human=False,
                context={'hub': hub, 'exit_code': exit_code, 'log_snippet': logs[-2000:]}
            )
        
        # Generic failure
        return Diagnosis(
            error_type=ErrorType.UNKNOWN,
            description=f"Process failed with exit code {exit_code}",
            suggested_fix="Review logs for specific error",
            can_auto_retry=False,
            needs_human=True,
            context={'hub': hub, 'exit_code': exit_code, 'log_snippet': logs[-2000:]}
        )
