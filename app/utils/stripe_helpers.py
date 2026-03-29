"""
Helpers for extracting data from Stripe payment/transaction objects.
Used by sync, webhooks, and API to consistently parse email and other fields.
"""
from typing import Optional


def extract_email_from_payment_raw(raw_event) -> Optional[str]:
    """
    Extract customer/receipt email from StripePayment.raw_event or payment object.
    Handles: Charge, PaymentIntent, Invoice (direct or webhook-wrapped).
    Checks: customer_email, receipt_email, billing_details.email, data.object.*
    """
    if not raw_event:
        return None
    d = raw_event if isinstance(raw_event, dict) else {}
    email = d.get("customer_email") or d.get("receipt_email")
    if email:
        return email
    billing = d.get("billing_details") or {}
    if isinstance(billing, dict):
        email = billing.get("email")
        if email:
            return email
    obj = d.get("data")
    if isinstance(obj, dict):
        obj = obj.get("object", {})
    if isinstance(obj, dict):
        email = obj.get("customer_email") or obj.get("receipt_email")
        if email:
            return email
        billing = obj.get("billing_details") or {}
        if isinstance(billing, dict):
            email = billing.get("email")
            if email:
                return email
    return None


def extract_email_from_payment_data(payment_data) -> Optional[str]:
    """
    Extract email from Stripe API object (Charge, PaymentIntent, Invoice).
    Use when payment_data is the live Stripe object (has getattr).
    """
    if not payment_data:
        return None
    email = getattr(payment_data, "customer_email", None) or getattr(payment_data, "receipt_email", None)
    if email:
        return email
    billing = getattr(payment_data, "billing_details", None)
    if billing:
        if isinstance(billing, dict):
            email = billing.get("email")
        else:
            email = getattr(billing, "email", None)
        if email:
            return email
    return None
