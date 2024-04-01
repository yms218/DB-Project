from sys import argv
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.movielens

def q3(input_title: str):
    # TODO: 
    client = MongoClient('mongodb://localhost:27017/')
    db = client.movielens
    # ml_movies에서 입력받은 title로 검색한 document의 movieId 값을 찾기
    movie_id = db.ml_movies.find_one({'title': input_title}, {'movieId': 1})

    if movie_id:
        # 해당 movieId로 ml_ratings에서 해당 영화의 평점을 가져옵니다.
        ratings = [doc['rating'] for doc in db.ml_ratings.find({'movieId': movie_id['movieId']}, {'rating': 1})]

        # 평균 평점을 계산합니다.
        average_rating = sum(ratings) / len(ratings) if ratings else None
        return print(f"{average_rating:.3f}")
    else:
        print("영화를 찾을 수 없습니다.")
        return None




if __name__ == '__main__':
    q3(argv[1])

