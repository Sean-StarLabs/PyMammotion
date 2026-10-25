import asyncio
import base64
import hashlib
import hmac
import json
from types import SimpleNamespace

import pytest

from pymammotion.http import http as http_module
from pymammotion.http.http import MammotionHTTP, create_oauth_signature
from pymammotion.transport.base import CredentialAttemptLimitError, LoginFailedError


class _Gate:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    async def claim(self, allow_after_success: bool = False) -> None:
        if self.calls and not allow_after_success:
            raise CredentialAttemptLimitError(True)
        self.calls.append(allow_after_success)


class _Response:
    status = 200

    async def json(self) -> dict[str, object]:
        return {"code": 40103, "msg": "synthetic client rejection"}


class _Session:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def post(self, url: str, **kwargs: object) -> _Response:
        self.calls.append((url, kwargs))
        return _Response()


def test_synthetic_oauth_signature_fixture() -> None:
    request = {
        "username": "synthetic@example.invalid",
        "password": base64.b64encode("möp".encode("utf-8")).decode("utf-8"),
        "client_id": "fixture-client",
        "grant_type": "password",
        "authType": "0",
    }
    signature = create_oauth_signature(
        request,
        "fixture-client",
        "fixture-secret",
        "/oauth2/token",
        1700000000123,
    )
    assert signature == "108f90a47266d03dba2add8f9da845798238b46d042eb78068639e645ce8e6ff"

    serialized = json.dumps(request, ensure_ascii=False, separators=(",", ":"))
    signed = f"fixture-client1700000000123/oauth2/token{serialized}"
    key = hashlib.md5(b"fixture-secret").hexdigest().encode("utf-8")
    assert signature == hmac.new(key, signed.encode("utf-8"), hashlib.sha256).hexdigest()

    refresh_request = {
        "client_id": "fixture-client",
        "grant_type": "refresh_token",
        "refresh_token": "fixture-refresh-token",
    }
    refresh_signature = create_oauth_signature(
        refresh_request,
        "fixture-client",
        "fixture-secret",
        "/oauth2/token",
        1700000000456,
    )
    assert refresh_signature == "5df47c40a1ba4e576e96bd857ac67c5f4359d6ba1cc9ef7eef51a6caac84156a"


def test_login_failure_retains_server_response_code() -> None:
    error = LoginFailedError("synthetic-account", "synthetic rejection", code=40103)
    assert error.code == 40103


def test_login_request_uses_one_synthetic_auth_attempt(monkeypatch) -> None:
    monkeypatch.setattr(http_module.time, "time", lambda: 1700000000.123)
    monkeypatch.setattr(http_module.secrets, "randbelow", lambda _: 0)
    monkeypatch.setattr(http_module, "MAMMOTION_OAUTH2_CLIENT_ID", "fixture-client")
    monkeypatch.setattr(http_module, "MAMMOTION_OAUTH2_CLIENT_SECRET", "fixture-secret")
    monkeypatch.setattr(http_module, "MAMMOTION_DOMAIN", "https://fixture.invalid")

    gate = _Gate()
    session = _Session()
    client = MammotionHTTP(
        session=session,
        ha_version="0.6.4",
        on_credential_attempt=gate,
    )

    async def run() -> None:
        response = await client.login_v2("synthetic@example.invalid", "möp")
        assert response.code == 40103
        assert len(session.calls) == 1
        url, request = session.calls[0]
        assert url == "https://fixture.invalid/oauth2/token"
        assert list(request["params"]) == [
            "username",
            "password",
            "client_id",
            "grant_type",
            "authType",
        ]
        assert request["params"]["password"] == "bcO2cA=="
        headers = request["headers"]
        assert headers["Ma-App-Key"] == "fixture-client"
        assert headers["Ma-Timestamp"] == "1700000000123"
        assert headers["Client-Id"] == "1700000000123_0000000_1"
        assert headers["Client-Type"] == "1"
        assert headers["App-Version"] == "HA,2.0.6.4"
        assert gate.calls == [False]

        with pytest.raises(CredentialAttemptLimitError):
            await client.login_v2("synthetic@example.invalid", "möp")
        assert len(session.calls) == 1

    asyncio.run(run())


def test_refresh_request_uses_synthetic_refresh_contract(monkeypatch) -> None:
    monkeypatch.setattr(http_module.time, "time", lambda: 1700000000.456)
    monkeypatch.setattr(http_module.secrets, "randbelow", lambda _: 0)
    monkeypatch.setattr(http_module, "MAMMOTION_OAUTH2_CLIENT_ID", "fixture-client")
    monkeypatch.setattr(http_module, "MAMMOTION_OAUTH2_CLIENT_SECRET", "fixture-secret")
    monkeypatch.setattr(http_module, "MAMMOTION_DOMAIN", "https://fixture.invalid")

    gate = _Gate()
    session = _Session()
    client = MammotionHTTP(
        session=session,
        ha_version="0.6.4",
        on_credential_attempt=gate,
    )
    client._login_info = SimpleNamespace(
        access_token="fixture-access-token",
        refresh_token="fixture-refresh-token",
    )

    async def run() -> None:
        response = await client.refresh_token_v2()
        assert response.code == 40103
        assert len(session.calls) == 1
        url, request = session.calls[0]
        assert url == "https://fixture.invalid/oauth2/token"
        assert list(request["params"]) == [
            "client_id",
            "grant_type",
            "refresh_token",
        ]
        assert request["params"]["grant_type"] == "refresh_token"
        headers = request["headers"]
        assert headers["Ma-App-Key"] == "fixture-client"
        assert headers["Ma-Timestamp"] == "1700000000456"
        assert headers["Client-Id"] == "1700000000456_0000000_1"
        assert headers["Client-Type"] == "1"
        assert gate.calls == [True]

    asyncio.run(run())
