from dataclasses import dataclass

@dataclass
class Review():
    review_id: int
    title: str
    content: str
    rating: int