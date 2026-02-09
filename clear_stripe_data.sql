-- SQL script to clear all Stripe-related data from the database
-- WARNING: This will delete ALL Stripe data for ALL organizations
-- Run this script with caution!

-- Option 1: Clear data for ALL organizations (uncomment to use)
-- DELETE FROM stripe_treasury_transactions;
-- DELETE FROM stripe_payments;
-- DELETE FROM stripe_subscriptions;
-- DELETE FROM stripe_events;

-- Option 2: Clear data for a specific organization (replace 'YOUR_ORG_ID' with actual UUID)
-- DELETE FROM stripe_treasury_transactions WHERE org_id = 'YOUR_ORG_ID'::UUID;
-- DELETE FROM stripe_payments WHERE org_id = 'YOUR_ORG_ID'::UUID;
-- DELETE FROM stripe_subscriptions WHERE org_id = 'YOUR_ORG_ID'::UUID;
-- DELETE FROM stripe_events WHERE org_id = 'YOUR_ORG_ID'::UUID;

-- Check counts before deletion (uncomment to see what will be deleted)
-- SELECT 'stripe_payments' as table_name, COUNT(*) as count FROM stripe_payments
-- UNION ALL
-- SELECT 'stripe_subscriptions', COUNT(*) FROM stripe_subscriptions
-- UNION ALL
-- SELECT 'stripe_events', COUNT(*) FROM stripe_events
-- UNION ALL
-- SELECT 'stripe_treasury_transactions', COUNT(*) FROM stripe_treasury_transactions;


