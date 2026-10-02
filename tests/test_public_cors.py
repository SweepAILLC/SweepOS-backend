"""PublicTrackingCorsMiddleware: /funnels/events and /funnels/leads answer any
origin without credentials; every other route keeps the allow-list.

The app below mirrors main.py's stack order (allow-list CORSMiddleware, then the
public middleware registered outermost) without importing main.py's startup."""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from app.middleware.public_cors import PublicTrackingCorsMiddleware, is_public_cors_path

ALLOWED = "http://localhost:3003"
UNKNOWN = "https://funnel.client-ghl-domain.example"


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/funnels/events", status_code=202)
    def events():
        return {"status": "accepted"}

    @app.post("/funnels/leads", status_code=201)
    def leads():
        raise HTTPException(status_code=404, detail="Funnel not found")

    @app.get("/funnels")
    def private():
        return []

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[ALLOWED],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(PublicTrackingCorsMiddleware)
    return app


def _preflight(client, path, origin=UNKNOWN, method="POST"):
    return client.options(
        path,
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": method,
            "Access-Control-Request-Headers": "content-type",
        },
    )


client = TestClient(_app())


def test_preflight_from_unknown_origin_passes_on_public_routes():
    for path in ("/funnels/events", "/funnels/leads", "/funnels/events/"):
        r = _preflight(client, path)
        assert r.status_code == 204, path
        assert r.headers["access-control-allow-origin"] == "*"
        assert "POST" in r.headers["access-control-allow-methods"]
        assert r.headers["access-control-allow-headers"].lower() == "content-type"
        assert "access-control-allow-credentials" not in r.headers


def test_post_from_unknown_origin_gets_wildcard_and_no_credentials():
    r = client.post("/funnels/events", json={}, headers={"Origin": UNKNOWN})
    assert r.status_code == 202
    assert r.headers["access-control-allow-origin"] == "*"
    assert "access-control-allow-credentials" not in r.headers


def test_allowed_origin_on_public_route_is_also_wildcard_without_credentials():
    # The allow-list middleware would echo the origin + credentials=true; we replace it.
    r = client.post("/funnels/events", json={}, headers={"Origin": ALLOWED})
    assert r.headers["access-control-allow-origin"] == "*"
    assert "access-control-allow-credentials" not in r.headers


def test_error_responses_on_public_route_keep_cors_headers():
    r = client.post("/funnels/leads", json={}, headers={"Origin": UNKNOWN})
    assert r.status_code == 404
    assert r.headers["access-control-allow-origin"] == "*"


def test_private_route_keeps_allow_list():
    r = _preflight(client, "/funnels", method="GET")
    assert r.status_code == 400
    assert "access-control-allow-origin" not in r.headers

    r = client.get("/funnels", headers={"Origin": UNKNOWN})
    assert "access-control-allow-origin" not in r.headers

    r = client.get("/funnels", headers={"Origin": ALLOWED})
    assert r.headers["access-control-allow-origin"] == ALLOWED
    assert r.headers["access-control-allow-credentials"] == "true"


def test_path_matching_is_exact():
    assert is_public_cors_path("/funnels/events")
    assert is_public_cors_path("/funnels/leads/")
    assert not is_public_cors_path("/funnels/events/explore")
    assert not is_public_cors_path("/funnels/abc/leads")
    assert not is_public_cors_path("/funnels")
