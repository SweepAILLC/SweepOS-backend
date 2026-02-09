"""
Send onboarding invitation emails via Brevo using BREVO_API_KEY.
This is separate from per-org Brevo OAuth/API; it does not touch existing integrations.
"""
from typing import Optional
import httpx
from app.core.config import settings


def send_onboarding_email(
    to_email: str,
    subject: str,
    html_content: str,
    *,
    to_name: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> bool:
    """
    Send a single transactional email using Brevo API with BREVO_API_KEY.
    Returns True if sent successfully, False otherwise (e.g. BREVO_API_KEY not set).
    """
    api_key = getattr(settings, "BREVO_API_KEY", None)
    if not api_key or not str(api_key).strip():
        return False
    api_key = str(api_key).strip()

    sender_email = getattr(settings, "SUDO_ADMIN_EMAIL", None) or "noreply@sweepos.local"
    sender = {
        "name": "Sweep OS",
        "email": sender_email,
    }
    to_list = [{"email": to_email.strip().lower(), "name": (to_name or "").strip() or None}]
    payload = {
        "sender": sender,
        "to": to_list,
        "subject": subject,
        "htmlContent": html_content,
    }
    if reply_to:
        payload["replyTo"] = {"email": reply_to, "name": "Sweep OS"}

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key,
    }
    try:
        resp = httpx.post(
            "https://api.brevo.com/v3/smtp/email",
            headers=headers,
            json=payload,
            timeout=15.0,
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False


INVITATION_EXPIRES_DAYS = 7


def send_org_admin_invitation_email(to_email: str, org_name: str, invitation_link: str) -> bool:
    """Send email inviting someone to set up an organization as org admin."""
    subject = f"You've been invited to set up {org_name} on Sweep OS"
    html = f"""
    <p>You've been invited to set up <strong>{org_name}</strong> on Sweep OS.</p>
    <p>Click the link below to create your account and set your password. This link expires in {INVITATION_EXPIRES_DAYS} days.</p>
    <p><a href="{invitation_link}">{invitation_link}</a></p>
    <p>After you sign in, you can add the system owner to your organization from the Owner tab.</p>
    <p>If you didn't expect this email, you can ignore it.</p>
    """
    return send_onboarding_email(to_email, subject, html)


def send_user_invitation_email(
    to_email: str,
    org_name: str,
    invitation_link: str,
    role: str,
    inviter_name: Optional[str] = None,
    existing_user: bool = False,
) -> bool:
    """Send email inviting a user to join an organization (new or existing user)."""
    subject = f"You've been invited to join {org_name} on Sweep OS"
    inviter = inviter_name or "A team admin"
    if existing_user:
        body_extra = "<p>You already have an account. Click the link below to accept and join this organization.</p>"
    else:
        body_extra = "<p>Click the link below to create your account and set your password. You'll be added as a <strong>" + role + "</strong>.</p>"
    html = f"""
    <p>{inviter} has invited you to join <strong>{org_name}</strong> on Sweep OS.</p>
    {body_extra}
    <p>This link expires in {INVITATION_EXPIRES_DAYS} days.</p>
    <p><a href="{invitation_link}">{invitation_link}</a></p>
    <p>If you didn't expect this email, you can ignore it.</p>
    """
    return send_onboarding_email(to_email, subject, html)
