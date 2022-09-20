from fastapi import FastAPI

from app.dto.Review import Review
from app.dto.Reviews import Reviews
from app.dto.ReviewRequest import ReviewRequest

from app.service.Reader import Reader
from app.service.Writer import Writer

app = FastAPI()

@app.get("/v1")
def read_root():
    return {"Hello": "World"}

@app.get("/v1/{company_id}/reviews")
def read_item(company_id: int) -> Reviews:
    with Reader() as reader:
        return reader.getCompanyReviews(company_id)

@app.post("/v1/{company_id}/reviews")
def post_item(company_id: int, request: ReviewRequest) -> Review:
    with Writer() as writer:
        return writer.postReview(company_id, request)
