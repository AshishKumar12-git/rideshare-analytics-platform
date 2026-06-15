from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from auth import verify_token

security = HTTPBearer()


def get_current_client(
    credentials: HTTPAuthorizationCredentials = Depends(
        security
    )
):

    token = credentials.credentials

    try:

        payload = verify_token(
            token
        )

        return payload

    except Exception:

        raise HTTPException(
            status_code=401,
            detail="Invalid or Expired Token"
        )