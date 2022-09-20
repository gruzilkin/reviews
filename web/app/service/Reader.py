from dataclasses import dataclass
from typing import Dict, List
from app.service.Memcached import getMemcachedClient
from app.service.Cassandra import getCassandraCluster
from app.dto.Reviews import Reviews
from app.dto.Review import Review

class Reader:
    def __init__(self) -> None:
        pass        

    def __enter__(self):
        self.cassandra = getCassandraCluster().connect('reviews')
        self.memcached = getMemcachedClient()
        return self
  
    def __exit__(self, exc_type, exc_value, traceback):
        #self.cassandra.shutdown()
        self.memcached.close()

    def __getCompanyDataVersion(self, company_id: int) -> int:
        key = f"company_id_{company_id}_version"
        version = self.memcached.get(key)
        if not version:
            (version,) = self.cassandra.execute(
                        """
                        SELECT MAX(writetime(content)) as version
                        FROM reviews
                        WHERE company_id = %s
                        """,(company_id,)
                    ).one()
            self.memcached.add(key, version)

        print(f"latest version in cache is {version}")
        return version

    def __getCompanyReviewIds(self, company_id) -> List[int]:
        version = self.__getCompanyDataVersion(company_id)
        key = f"company_id_{company_id}_review_ids_{version}"
        review_ids = self.memcached.get(key)
        if not review_ids:
            rows = self.cassandra.execute(
                        """
                        SELECT review_id
                        FROM reviews
                        WHERE company_id = %s
                        """,(company_id,)
                    )
            
            review_ids = []
            for row in rows:
                (review_id,) = row
                review_ids.append(review_id)

            self.memcached.set(key, review_ids)

        return review_ids

    def __loadCompanyReviews(self, company_id, review_ids: List[int]) -> Dict[int, Review]:
        rows = self.cassandra.execute(f"""
            SELECT review_id, title, content, rating
            FROM reviews
            WHERE company_id = %s AND review_id IN ({",".join(["%s"] * len(review_ids))})
            """, (company_id, *review_ids))
        reviews = dict()
        for review_id, title, content, rating in rows:
            reviews[review_id] = Review(review_id, title, content, rating)
        return reviews

    def getCompanyReviews(self, company_id) -> Reviews:
        review_ids = self.__getCompanyReviewIds(company_id)
        cache = self.memcached.get_many([str(review_id) for review_id in review_ids])
        missing_review_ids = [review_id for review_id in review_ids if str(review_id) not in cache]
        missing_reviews = self.__loadCompanyReviews(company_id, missing_review_ids)
        self.memcached.set_many({str(key):value for key, value in missing_reviews.items()})

        reviews = []
        print(f"review_ids of type {type(review_ids)}: {review_ids}")
        for review_id in review_ids:
            if str(review_id) in cache:
                review = cache[str(review_id)]
                review.source = "cache"
                reviews.append(review)
            else:
                review = missing_reviews[review_id]
                review.source = "cassandra"
                reviews.append(review)
        return Reviews(reviews)