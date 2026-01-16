# API Routing Guidelines

## Router Registration Order

**CRITICAL**: FastAPI matches routes in the order routers are registered. If two routers have overlapping paths, the **first registered router** will match first.

### Current Router Registration Order (in `main.py`)

```python
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(clients.router, prefix="/clients", tags=["clients"])
app.include_router(events.router, prefix="/events", tags=["events"])
app.include_router(oauth.router, prefix="/oauth", tags=["oauth"])
app.include_router(integrations.router, prefix="/integrations", tags=["integrations"])
app.include_router(stripe.router, prefix="/integrations/stripe", tags=["stripe"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
```

### Potential Conflicts

**FIXED**: The following conflict was resolved:
- ❌ **CONFLICT**: `/integrations/stripe/summary`
  - `integrations.py` → `/integrations/stripe/summary` (old mock endpoint)
  - `stripe.py` → `/integrations/stripe/summary` (real implementation)
  - **Resolution**: Removed duplicate endpoint from `integrations.py`

### Rules to Prevent Conflicts

1. **More specific prefixes should be registered AFTER less specific ones**
   - ✅ Correct: `/integrations` before `/integrations/stripe`
   - ❌ Wrong: `/integrations/stripe` before `/integrations`

2. **Never create duplicate endpoints in different routers**
   - If you need to move an endpoint, remove it from the old location
   - Document the move in a comment

3. **Check for conflicts before adding new routes**
   - Search for the full path across all router files
   - Use: `grep -r "router.get\|router.post" backend/app/api/`

4. **Old/unused router files should be removed or clearly marked**
   - Files like `stripe_old.py` and `stripe_v2.py` are NOT imported in `main.py`
   - They should be removed or moved to an archive folder to avoid confusion
   - **Current status**: These files exist but are not causing conflicts (not imported)

### Testing for Conflicts

To check for routing conflicts:

```bash
# Use the automated check script
make check-routing

# Or manually:
docker-compose -f docker/docker-compose.yml exec backend python scripts/check_routing_conflicts.py

# List all registered routes via OpenAPI
curl http://localhost:8000/docs  # Check OpenAPI docs
```

### Best Practices

1. **One endpoint per path**: Each unique path should exist in only one router
2. **Clear separation**: Keep related endpoints in the same router file
3. **Documentation**: Add comments when moving endpoints
4. **Cleanup**: Remove old/unused router files to avoid confusion

