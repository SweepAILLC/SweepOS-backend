"""
Email spam detection service.
Uses rule-based heuristics and optionally AI/ML for sophisticated detection.
"""
from typing import Dict, Any, Optional
import re
from email.utils import parseaddr
import dns.resolver
import socket


class SpamDetectionResult:
    """Result of spam detection analysis"""
    def __init__(
        self,
        is_spam: bool,
        confidence: float,
        reasons: list[str],
        score: float = 0.0
    ):
        self.is_spam = is_spam
        self.confidence = confidence  # 0.0 to 1.0
        self.reasons = reasons
        self.score = score  # Spam score (higher = more likely spam)


class EmailSpamDetector:
    """
    Detects spam/marketing emails using rule-based heuristics.
    Can be extended with AI/ML models for more sophisticated detection.
    """
    
    # Common spam/marketing email patterns
    SPAM_DOMAIN_PATTERNS = [
        r'^noreply@',
        r'^no-reply@',
        r'^donotreply@',
        r'^mailer@',
        r'^marketing@',
        r'^newsletter@',
        r'^notifications@',
        r'^alerts@',
        r'^system@',
        r'^automated@',
    ]
    
    # Spam keywords in email addresses
    SPAM_KEYWORDS = [
        'noreply', 'no-reply', 'donotreply', 'mailer', 'marketing',
        'newsletter', 'notification', 'alert', 'system', 'automated',
        'support', 'info', 'sales', 'hello', 'contact', 'team'
    ]
    
    # Common marketing email domains
    KNOWN_MARKETING_DOMAINS = [
        'mailchimp.com', 'constantcontact.com', 'sendgrid.com',
        'campaignmonitor.com', 'getresponse.com', 'aweber.com',
        'mailgun.com', 'sendinblue.com', 'brevo.com', 'hubspot.com'
    ]
    
    # Disposable email domains (common ones)
    DISPOSABLE_EMAIL_DOMAINS = [
        'tempmail.com', '10minutemail.com', 'guerrillamail.com',
        'mailinator.com', 'throwaway.email', 'temp-mail.org'
    ]
    
    def __init__(self, use_ai: bool = False, ai_model: Optional[Any] = None):
        """
        Initialize spam detector.
        
        Args:
            use_ai: Whether to use AI/ML model for detection
            ai_model: Optional AI model for sophisticated detection
        """
        self.use_ai = use_ai
        self.ai_model = ai_model
        self.spam_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.SPAM_DOMAIN_PATTERNS]
    
    def detect_spam(
        self,
        email_address: str,
        sender_name: Optional[str] = None,
        subject: Optional[str] = None,
        body: Optional[str] = None
    ) -> SpamDetectionResult:
        """
        Detect if an email is spam/marketing.
        
        Args:
            email_address: Email address to check
            sender_name: Optional sender name
            subject: Optional email subject
            body: Optional email body
            
        Returns:
            SpamDetectionResult with detection outcome
        """
        score = 0.0
        reasons = []
        
        # Normalize email
        email_lower = email_address.lower().strip()
        name, domain = parseaddr(email_lower)[1].split('@') if '@' in email_lower else ('', '')
        
        if not domain:
            return SpamDetectionResult(
                is_spam=True,
                confidence=1.0,
                reasons=["Invalid email format"],
                score=1.0
            )
        
        # Rule 1: Check for spam patterns in email address
        for pattern in self.spam_patterns:
            if pattern.match(email_lower):
                score += 0.3
                reasons.append(f"Email matches spam pattern: {pattern.pattern}")
        
        # Rule 2: Check for spam keywords in email address
        for keyword in self.SPAM_KEYWORDS:
            if keyword in email_lower:
                score += 0.2
                reasons.append(f"Contains spam keyword: {keyword}")
        
        # Rule 3: Check if domain is a known marketing service
        if domain in self.KNOWN_MARKETING_DOMAINS:
            score += 0.4
            reasons.append(f"Domain is known marketing service: {domain}")
        
        # Rule 4: Check if domain is disposable
        if domain in self.DISPOSABLE_EMAIL_DOMAINS:
            score += 0.5
            reasons.append(f"Disposable email domain: {domain}")
        
        # Rule 5: Check sender name patterns
        if sender_name:
            sender_lower = sender_name.lower()
            if any(keyword in sender_lower for keyword in ['team', 'marketing', 'newsletter', 'automated']):
                score += 0.2
                reasons.append(f"Sender name suggests marketing: {sender_name}")
        
        # Rule 6: Check subject line patterns
        if subject:
            subject_lower = subject.lower()
            spam_subject_keywords = [
                'unsubscribe', 'opt-out', 'special offer', 'limited time',
                'act now', 'click here', 'free trial', 'discount', 'sale',
                'promotion', 'newsletter', 'update', 'alert'
            ]
            if any(keyword in subject_lower for keyword in spam_subject_keywords):
                score += 0.15
                reasons.append("Subject contains marketing keywords")
        
        # Rule 7: Check for generic email addresses (info@, support@, etc.)
        generic_prefixes = ['info', 'support', 'sales', 'contact', 'hello', 'help']
        if name in generic_prefixes:
            score += 0.25
            reasons.append(f"Generic email prefix: {name}")
        
        # Rule 8: Check domain reputation (basic DNS check)
        try:
            # Check if domain has MX records (legitimate domains usually do)
            mx_records = dns.resolver.resolve(domain, 'MX')
            if not mx_records:
                score += 0.1
                reasons.append("Domain has no MX records")
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, Exception):
            score += 0.2
            reasons.append("Domain DNS check failed or domain doesn't exist")
        
        # Rule 9: Check for suspicious patterns (numbers, random strings)
        if re.match(r'^[a-z0-9]{20,}@', email_lower):
            score += 0.15
            reasons.append("Email has suspicious random pattern")
        
        # Normalize score to 0-1 range
        score = min(score, 1.0)
        
        # Determine if spam (threshold: 0.4)
        is_spam = score >= 0.4
        confidence = min(score, 1.0) if is_spam else (1.0 - score)
        
        # If AI is enabled, use it for final decision
        if self.use_ai and self.ai_model:
            ai_result = self._ai_detect(email_address, sender_name, subject, body)
            # Combine AI result with rule-based score
            combined_score = (score * 0.6) + (ai_result.score * 0.4)
            is_spam = combined_score >= 0.4
            confidence = min(combined_score, 1.0) if is_spam else (1.0 - combined_score)
            if ai_result.reasons:
                reasons.extend(ai_result.reasons)
        
        return SpamDetectionResult(
            is_spam=is_spam,
            confidence=confidence,
            reasons=reasons,
            score=score
        )
    
    def _ai_detect(
        self,
        email_address: str,
        sender_name: Optional[str] = None,
        subject: Optional[str] = None,
        body: Optional[str] = None
    ) -> SpamDetectionResult:
        """
        AI/ML-based spam detection.
        This is a placeholder - implement with your preferred ML model.
        
        Options:
        - OpenAI API for classification
        - Local ML model (scikit-learn, TensorFlow, etc.)
        - Third-party spam detection API
        """
        # Placeholder implementation
        # TODO: Implement AI/ML model here
        return SpamDetectionResult(
            is_spam=False,
            confidence=0.0,
            reasons=[],
            score=0.0
        )
    
    def is_real_person(self, email_address: str, sender_name: Optional[str] = None) -> bool:
        """
        Quick check if email appears to be from a real person.
        Inverse of spam detection.
        """
        result = self.detect_spam(email_address, sender_name)
        return not result.is_spam


# Convenience function
def detect_spam_email(
    email_address: str,
    sender_name: Optional[str] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    use_ai: bool = False
) -> SpamDetectionResult:
    """
    Convenience function to detect spam emails.
    
    Usage:
        result = detect_spam_email("noreply@example.com")
        if result.is_spam:
            print(f"Spam detected: {result.reasons}")
    """
    detector = EmailSpamDetector(use_ai=use_ai)
    return detector.detect_spam(email_address, sender_name, subject, body)


