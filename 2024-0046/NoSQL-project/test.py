'''
------------------------------------------------------------------
Idenx 성능 검증방법 및 결과

하기와 같은 방법으로 Index 효율성을 검증하였습니다.
1. DB내 모든 Index 삭제 - 현 DB내 Index 출력
2. q2, q3, q4 실행 후 결과 값 출력
3. Index 생성 - q2, q3, q4 각 2개씩필요하며 중복이라 5개생성
5. 현 DB내 Index 출력
6. q2, q3, q4 실행 후 결과 값 출력

결과는 3개 함수 평균 약 7초 Gain 확인됩니다.
q4는 파이썬으로 처리하는 비중이 높아 전 후 개선 비율이 낮습니다.
시간 관계상 n번 반복 실행 후 평균값을 낸 것이 아니라 오차는 존재합니다.

------------------------------------------------------------------
코드 실행 시 출력내용

Index 정보 (삭제 후):
Indexes for ml_movies: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_movies'}}
Indexes for ml_tags: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_tags'}}
Indexes for ml_ratings: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_ratings'}}

q2 실행 결과: ['Bad Education (La mala educación) (2004)', 'Mammoth (Mammut) (2009)']
q3 실행 결과: 4.097927896085535
q4 실행 결과: -0.580230297564422

Index 정보 (생성 후):
Indexes for ml_movies: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_movies'}, 'movieId_1_title_1': {'v': 2, 'key': [('movieId', 1), ('title', 1)], 'ns': 'movielens.ml_movies'}}
Indexes for ml_tags: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_tags'}, 'tag_1': {'v': 2, 'key': [('tag', 1)], 'ns': 'movielens.ml_tags'}}
Indexes for ml_ratings: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'movielens.ml_ratings'}, 'movieId_1_userId_1': {'v': 2, 'key': [('movieId', 1), ('userId', 1)], 'ns': 'movielens.ml_ratings'}}

q2 실행 결과: ['Bad Education (La mala educación) (2004)', 'Mammoth (Mammut) (2009)']
q3 실행 결과: 4.097927896085535
q4 실행 결과: -0.580230297564422


쿼리 실행 전 후 시간 차이:
--------------------
Question2:
Index 생성 전 시간: 0.513초
Index 생성 후 시간: 0.014초
전 후 차이 시간: 0.499초
--------------------
Question3:
Index 생성 전 시간: 11.822초
Index 생성 후 시간: 0.671초
전 후 차이 시간: 11.151초
--------------------
Question4:
Index 생성 전 시간: 32.984초
Index 생성 후 시간: 23.346초
전 후 차이 시간: 9.638초
--------------------

평균 차이값: 7.096초
------------------------------------------------------------------
'''




from pymongo import MongoClient
import time

# MongoDB 연결 설정
client = MongoClient('mongodb://localhost:27017/')
db = client['movielens']  # 데이터베이스 선택

# 인덱스 생성 함수
def create_index(collection, index_spec):
    if index_spec is not None:
        collection.create_index(index_spec)

# 인덱스 삭제 함수
def drop_all_indexes(db):
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        collection.drop_indexes()

# 쿼리 함수 정의
def q2(tag):
    # ml_tags에서 해당 tag를 가진 document의 movieId들 가져오기
    movie_ids = db.ml_tags.distinct("movieId", {"tag": tag})

    # ml_movies에서 해당 movieId들로 document의 title들 가져오기 (중복 없이 오름차순 정렬)
    titles = db.ml_movies.find({"movieId": {"$in": movie_ids}}, {"title": 1, "_id": 0}).sort("title", 1)
    unique_titles = sorted(set(doc["title"] for doc in titles))

    return unique_titles        


def q3(title):
    # ml_movies에서 입력받은 title로 검색한 document의 movieId 값을 찾기
    movie_id = db.ml_movies.find_one({'title': title}, {'movieId': 1})

    if movie_id:
        # 해당 movieId로 ml_ratings에서 해당 영화의 평점을 가져옵니다.
        ratings = [doc['rating'] for doc in db.ml_ratings.find({'movieId': movie_id['movieId']}, {'rating': 1})]

        # 평균 평점을 계산합니다.
        average_rating = sum(ratings) / len(ratings) if ratings else None
        return average_rating
    else:
        print("영화를 찾을 수 없습니다.")
        return None




def q4(userId):
    # 특정 userId가 평가한 영화의 movieId 목록 구하기
    movie_ids = db.ml_ratings.distinct("movieId", {"userId": userId})

    # 특정 movieIds에 해당하는 문서에서 movieId, rating만 가져오기
    ratings_cursor = db.ml_ratings.find(
        {"movieId": {"$in": movie_ids}},
        {"_id": 0, "movieId": 1, "rating": 1}
    )    
    # movieId별로 평점을 저장할 딕셔너리 초기화
    ratings_by_movie = {}

    # 검색된 결과를 반복하여 각 영화별로 평점 리스트 구성
    for rating_doc in ratings_cursor:
        movie_id = rating_doc['movieId']
        rating = rating_doc['rating']
        # 해당 movieId의 평점 리스트가 이미 딕셔너리에 있으면 추가, 없으면 새로운 리스트 생성
        ratings_by_movie.setdefault(movie_id, []).append(rating)    

    # 각 movieId별 평균 평점 계산
    movie_avg_ratings_dict = {}
    for movie_id, ratings in ratings_by_movie.items():
        avg_rating = sum(ratings) / len(ratings) if ratings else 0  # 평점 평균 계산
        movie_avg_ratings_dict[movie_id] = avg_rating  # 결과 딕셔너리에 저장

    # userId로 제한하여 해당 사용자의 영화별 평점 구하기
    user_ratings_result = db.ml_ratings.find({'userId': userId}, {'movieId': 1, 'rating': 1})

    # userId가 평가한 영화의 개수와 각 영화의 평점과 평균 평점의 차이를 누적할 변수 초기화
    total_bias = 0
    num_rated_movies = len(movie_ids)

    # 각 영화의 평점과 평균 평점의 차이를 계산하여 누적
    for user_rating in user_ratings_result:
        movie_id = user_rating['movieId']
        rating = user_rating['rating']
        avg_rating_by_movie = movie_avg_ratings_dict.get(movie_id, 0)
        total_bias += rating - avg_rating_by_movie

    # userId별 평균 평점의 바이어스를 반환
    return total_bias / num_rated_movies if num_rated_movies > 0 else 0








def q1():
    # 1. 인덱스 삭제
    drop_all_indexes(db)


    before_time = []
    # 2. 코드 실행 결과 출력 및 생성 전 시간 저장
    results_pre = {}
    for query_name, query_func, param in [('q2', q2, "Gael Garcia Bernal"), ('q3', q3, "Interstellar (2014)"), ('q4', q4, 121120)]:
        start_time = time.time()
        query_result = query_func(param)  # 함수 실행
        # 결과 처리 방식 변경
        if query_name == 'q3' or query_name == 'q4':  # q3, 4의 경우 단일 float 값을 직접 저장
            results_pre[query_name] = query_result
        else:  # 다른 쿼리 결과(list)를 처리
            results_pre[query_name] = list(query_result)
        end_time = time.time()
        print(f"{query_name} 실행 결과: {results_pre[query_name]}")
        before_time.append(end_time - start_time)

    # 3. 인덱스 생성
    create_index(db.ml_tags, [('tag', 1)])
    create_index(db.ml_movies, [('movieId', 1), ('title', 1)])
    create_index(db.ml_ratings, [('movieId', 1), ('userId', 1)])

    # 4. 코드 실행 결과 출력 및 생성 후 시간 저장
    results_post = {}
    
    after_time = []
    for query_name, query_func, param in [('q2', q2, "Gael Garcia Bernal"), ('q3', q3, "Interstellar (2014)"), ('q4', q4, 121120)]:
        start_time = time.time()
        query_result = query_func(param)  # 함수 실행
        # 결과 처리 방식 변경
        if query_name == 'q3' or query_name == 'q4':  # q3, 4의 경우 단일 float 값을 직접 저장
            results_post[query_name] = query_result
        else:  # 다른 쿼리 결과(list)를 처리
            results_post[query_name] = list(query_result)
        end_time = time.time()
        print(f"{query_name} 실행 결과: {results_post[query_name]}")
        after_time.append(end_time - start_time)

    print()
    # 쿼리 실행 전 후 시간 차이 계산
    diffs = [b-f for b, f in zip(before_time, after_time)]
    print("\n쿼리 실행 전 후 시간 차이:")
    
    for i in range(0, 3):
        print('-'*20)
        print(f"Question{i+2}:")
        print(f"Index 생성 전 시간: {before_time[i]:.3f}초")
        print(f"Index 생성 후 시간: {after_time[i]:.3f}초")
        print(f"전 후 차이 시간: {diffs[i]:.3f}초")
        i =+1
    

        


    # 평균 차이값 계산
    print('-'*20)
    avg_diff = sum(diffs) / len(diffs)
    print(f"\n평균 차이값: {avg_diff:.3f}초")



# 예시 실행
q1()


