from typing import TypedDict

class Response(TypedDict):
    """A response dict for the API Gateway"""
    statusCode: int
    body: str



