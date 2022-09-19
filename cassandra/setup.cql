CREATE KEYSPACE IF NOT EXISTS reviews WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE reviews;

CREATE TABLE IF NOT EXISTS reviews (
    company_id int,
    review_id bigint,
    title text,
    content text,
    rating int,
    PRIMARY KEY ((company_id), review_id)
) WITH CLUSTERING ORDER BY (review_id DESC);