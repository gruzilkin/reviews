from app.dto.Review import Review
from app.dto.ReviewRequest import ReviewRequest

from app.service.IdGenerator import getIdGenerator
from app.service.Kafka import getKafkaProducer

class Writer:
    def __init__(self) -> None:
        pass        

    def __enter__(self):
        self.kafka = getKafkaProducer()
        return self
  
    def __exit__(self, exc_type, exc_value, traceback):
        self.kafka.flush()

    def postReview(self, company_id: int, request: ReviewRequest) -> Review:
        idGenerator = getIdGenerator()
        id = idGenerator.next_id()
        message =  {"company_id": company_id, "review_id": id,
            "title": request.title, "content":request.content, "rating": request.rating}
        future = getKafkaProducer().send('reviews', message, key=company_id)
        future.get(timeout=60)
        
        return Review(id, request.title, request.content, request.rating, source="web")