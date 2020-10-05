import time
import secrets
from os import environ
from typing import Dict, Any
from urllib.parse import parse_qs

import jwt


def authorize(wsgi_environ) -> bool:
    if 'QUERY_STRING' not in wsgi_environ:
        return False

    try:
        query = parse_qs(wsgi_environ['QUERY_STRING'])
        if 'access_token' not in query:
            return False

        validate_jwt(query['access_token'][0])
        return True
    except ValueError:
        return False


def validate_jwt(token: str) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token,
                             environ['JWT_SECRET'],
                             algorithms=['HS256'])

        if any(key not in payload for key in ('sub', 'iss')):
            raise ValueError

        if payload['iss'] != 'boris-auth':
            raise ValueError

        # `pyjwt` had already validated `exp` value.
        return payload
    except jwt.PyJWTError:
        raise ValueError
    except ValueError:
        raise


def generate_message_id() -> str:
    prefix = str(time.time_ns())[2:]
    suffix = secrets.randbelow(2**16)
    return prefix + suffix
