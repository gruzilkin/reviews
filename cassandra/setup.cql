CREATE KEYSPACE IF NOT EXISTS reviews WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE reviews;

CREATE TABLE IF NOT EXISTS reviews (
    company_id int,
    review_id uuid,
    title text,
    content text,
    rating int,
    insert_date timeuuid,
    PRIMARY KEY ((company_id, review_id), insert_date)
) WITH CLUSTERING ORDER BY (insert_date DESC);