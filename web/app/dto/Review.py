from dataclasses import dataclass
from typing import Optional

@dataclass
class Review():
    review_id: int
    title: str
    content: str
    rating: int
    source: Optional[str] = None