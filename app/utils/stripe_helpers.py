"""
Helpers for extracting data from Stripe payment/transaction objects.
Used by sync, webhooks, and API to consistently parse email and other fields.
"""
from typing import Any, List, Optional


def _email_from_stripe_object(obj: Any) -> Optional[str]:
    """Pull email fields from a Stripe object dict (Charge, Invoice, PaymentIntent, etc.)."""
    if not obj or not isinstance(obj, dict):
        return None
    email = obj.get("customer_email") or obj.get("receipt_email")
    if email:
        return str(email).strip() or None
    billing = obj.get("billing_details") or {}
    if isinstance(billing, dict):
        email = billing.get("email")
        if email:
            return str(email).strip() or None
    # Expanded charges on PaymentIntent
    charges = obj.get("charges")
    if isinstance(charges, dict):
        data = charges.get("data") or []
        if data and isinstance(data[0], dict):
            e = _email_from_stripe_object(data[0])
            if e:
                return e
    return None


def extract_email_from_payment_raw(raw_event) -> Optional[str]:
    """
    Extract customer/receipt email from StripePayment.raw_event or payment object.
    Handles: Charge, PaymentIntent, Invoice (direct or webhook-wrapped).
    Checks: customer_email, receipt_email, billing_details.email, data.object.*,
    nested payment_intent, invoice, charges.data[].
    """
    if not raw_event:
        return None
    d = raw_event if isinstance(raw_event, dict) else {}
    email = _email_from_stripe_object(d)
    if email:
        return email
    obj = d.get("data")
    if isinstance(obj, dict):
        obj = obj.get("object", {})
    if isinstance(obj, dict):
        email = _email_from_stripe_object(obj)
        if email:
            return email
        # PaymentIntent nested on Charge
        pi = obj.get("payment_intent")
        if isinstance(pi, dict):
            email = _email_from_stripe_object(pi)
            if email:
                return email
        # Invoice expanded on Charge
        inv = obj.get("invoice")
        if isinstance(inv, dict):
            email = _email_from_stripe_object(inv)
            if email:
                return email
    return None


def collect_email_from_raw_events(raw_events: List[Any]) -> Optional[str]:
    """Try each stored raw payload (e.g. grouped retry attempts) until an email is found."""
    for raw in raw_events:
        if not raw:
            continue
        email = extract_email_from_payment_raw(raw)
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
