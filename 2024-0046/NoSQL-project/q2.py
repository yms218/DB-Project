from sys import argv
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.movielens



def q2(input_tag: str):
    # TODO: 
    client = MongoClient('mongodb://localhost:27017/')
    db = client.movielens
    movie_ids = db.ml_tags.distinct("movieId", {"tag": input_tag})

    # ml_movies에서 해당 movieId들로 document의 title들 가져오기 (중복 없이 오름차순 정렬)
    titles = db.ml_movies.find({"movieId": {"$in": movie_ids}}, {"title": 1, "_id": 0}).sort("title", 1)
    unique_titles = sorted(set(doc["title"] for doc in titles))

    print(*unique_titles, sep='\n')
        


if __name__ == '__main__':
    q2(argv[1])

