mongoimport -d movielens -c ml_movies ml-25m-json/movies.json --numInsertionWorkers=8
mongoimport -d movielens -c ml_ratings ml-25m-json/ratings.json --numInsertionWorkers=8
mongoimport -d movielens -c ml_tags ml-25m-json/tags.json --numInsertionWorkers=8
