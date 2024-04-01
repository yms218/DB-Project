


from sys import argv
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.movielens

def q4(input_userid: int):
    client = MongoClient('mongodb://localhost:27017/')
    db = client.movielens
    # TODO:# 특정 userId가 평가한 영화의 movieId 목록 구하기
    movie_ids = db.ml_ratings.distinct("movieId", {"userId": input_userid})

    # 특정 movieIds에 해당하는 문서에서 movieId, rating만 가져오기
    pipeline = [
        {"$match": {"movieId": {"$in": movie_ids}}},
        {"$group": {"_id": "$movieId", "avgRating": {"$avg": "$rating"}}}
    ]
    rating_aggregation = list(db.ml_ratings.aggregate(pipeline))

    # movieId별로 평균 평점을 저장할 딕셔너리 초기화
    movie_avg_ratings_dict = {rating['_id']: rating['avgRating'] for rating in rating_aggregation}

    # userId로 제한하여 해당 사용자의 영화별 평점 구하기
    user_ratings_cursor = db.ml_ratings.find({'userId': input_userid}, {'movieId': 1, 'rating': 1})

    # userId가 평가한 영화의 개수와 각 영화의 평점과 평균 평점의 차이를 누적할 변수 초기화
    total_bias = 0
    num_rated_movies = len(movie_ids)

    # 각 영화의 평점과 평균 평점의 차이를 계산하여 누적
    for user_rating in user_ratings_cursor:
        movie_id = user_rating['movieId']
        rating = user_rating['rating']
        avg_rating_by_movie = movie_avg_ratings_dict.get(movie_id, 0)
        total_bias += rating - avg_rating_by_movie

    # userId별 평균 평점의 바이어스를 반환
    total_bias / num_rated_movies if num_rated_movies > 0 else 0

    bias = total_bias / num_rated_movies if num_rated_movies > 0 else 0
    print('{:.3f}'.format(bias))



if __name__ == '__main__':
    q4(int(argv[1]))

