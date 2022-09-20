from dataclasses import dataclass
from typing import List

from app.dto.Review import Review

@dataclass
class Reviews():
    reviews: List[Review]