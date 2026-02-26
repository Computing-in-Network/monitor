from __future__ import annotations

from fastapi import Header, HTTPException, status
from starlette.datastructures import Headers


def validate_api_token(headers: Headers, expect_token: str) -> str:
    token = headers.get("x-api-token") or headers.get("authorization")
    if not expect_token:
        return "no-token-configured"

    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少 x-api-token",
        )

    if token == expect_token:
        return token

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="鉴权失败：token 不匹配",
    )

