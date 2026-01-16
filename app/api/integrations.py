from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.integration import BrevoStatus
from app.api.deps import get_current_user
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.core.encryption import decrypt_token
from datetime import datetime
import httpx

router = APIRouter()


# NOTE: Stripe summary endpoint has been moved to backend/app/api/stripe.py
# This endpoint is kept for backward compatibility but should not be used
# The new endpoint at /integrations/stripe/summary uses real database data from webhooks


@router.get("/brevo/status", response_model=BrevoStatus)
def get_brevo_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Brevo connection status and account information.
    Fetches real account data from Brevo API if connected.
    """
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.BREVO,
        OAuthToken.org_id == current_user.org_id
    ).first()
    
    if not brevo_token:
        return BrevoStatus(
            connected=False,
            message="Brevo not connected. Click 'Install Brevo' to connect."
        )
    
    # Check if token is expired
    is_expired = brevo_token.expires_at and brevo_token.expires_at < datetime.utcnow()
    if is_expired:
        return BrevoStatus(
            connected=False,
            message="Brevo token has expired. Please reconnect your account."
        )
    
    # Fetch real account info from Brevo API
    try:
        # Decrypt the access token
        access_token = decrypt_token(
            brevo_token.access_token,
            audit_context={
                "db": db,
                "org_id": current_user.org_id,
                "user_id": current_user.id,
                "resource_type": "brevo_token",
                "resource_id": str(brevo_token.id)
            }
        )
        
        # Call Brevo API to get account information
        # According to Brevo docs: GET https://api.brevo.com/v3/account
        response = httpx.get(
            "https://api.brevo.com/v3/account",
            headers={
                "Authorization": f"Bearer {access_token}",
                "accept": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code == 200:
            account_data = response.json()
            account_email = account_data.get("email")
            account_name = account_data.get("firstName") or account_data.get("companyName")
            if account_name and account_data.get("lastName"):
                account_name = f"{account_name} {account_data.get('lastName')}"
            
            return BrevoStatus(
                connected=True,
                account_email=account_email,
                account_name=account_name or "Connected Account",
                message="Brevo is connected and ready to use."
            )
        elif response.status_code == 401:
            # Token is invalid or expired
            return BrevoStatus(
                connected=False,
                message="Brevo token is invalid. Please reconnect your account."
            )
        else:
            # API error but token exists
            return BrevoStatus(
                connected=True,
                message=f"Brevo is connected but unable to fetch account details (API error: {response.status_code})."
            )
            
    except httpx.HTTPError as e:
        # Network error - token exists but can't verify
        return BrevoStatus(
            connected=True,
            message="Brevo is connected but unable to verify account details (network error)."
        )
    except Exception as e:
        # Other error - return basic status
        print(f"[BREVO] Error fetching account info: {str(e)}")
        return BrevoStatus(
            connected=True,
            message="Brevo is connected and ready to use."
        )

