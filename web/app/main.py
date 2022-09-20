from fastapi import FastAPI

from app.dto.Review import Review
from app.dto.Reviews import Reviews
from app.dto.ReviewRequest import ReviewRequest
from app.service.Memcached import getMemcachedClient
from app.service.Cassandra import getCassandraCluster
from app.service.IdGenerator import getIdGenerator
from app.service.Kafka import getKafkaProducer

app = FastAPI()

@app.get("/v1")
def read_root():
    return {"Hello": "World"}

@app.get("/v1/{company_id}/reviews")
def read_item(company_id: int) -> Reviews:
    with getCassandraCluster().connect('reviews') as session:
        memcachedClient = getMemcachedClient()
        company_version_key = f"company_id_{company_id}_version"
        version = memcachedClient.get(company_version_key)
        if not version:
            _, version = session.execute(
                        """
                        SELECT company_id, MAX(writetime(content)) as version
                        FROM reviews
                        WHERE company_id = %s
                        """,(company_id,)
                    ).one()
            memcachedClient.set(company_version_key, version)
        else:
            print(f"latest version in cache is {version}")

        rows = session.execute("""
            SELECT review_id, title, content, rating
            FROM reviews
            WHERE company_id = %s""", (company_id,))
        reviews = []
        for review_id, title, content, rating in rows:
            reviews.append(Review(review_id, title, content, rating))
        return Reviews(reviews)

@app.post("/v1/{company_id}/reviews")
def post_item(company_id: int, request: ReviewRequest) -> Review:
    idGenerator = getIdGenerator()
    id = idGenerator.next_id()
    message =  {"company_id": company_id, "review_id": id,
        "title": request.title, "content":request.content, "rating": request.rating}
    future = getKafkaProducer().send('reviews', message, key=company_id)
    future.get(timeout=60)
    
    review = Review(id, request.title, request.content, request.rating)
    print(review)
    return review

