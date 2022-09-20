from pydantic import BaseModel

class ReviewRequest(BaseModel):
    title: str
    content: str
    rating: int