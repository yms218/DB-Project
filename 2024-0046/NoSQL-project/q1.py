
'''
------------------------------------------------------------------
Idenx 설계 및 검증 결과

Index는 각 문제별로 처음 input값 필드 1개와 그 결과 값으로
재 검색하는 검색 필드 2개로 설계하여 진행했습니다.

하기와 같은 방법으로 Index 효율성을 검증하였습니다.
1. DB내 모든 Index 삭제 - 현 DB내 Index 출력
2. q2, q3, q4 실행 후 결과 값 출력
3. Index 생성 - q2, q3, q4 각 2개씩필요하며 중복이라 5개생성
5. 현 DB내 Index 출력
6. 기존시간대비 비율을 보면 q2(27%)와 q3(5%)는 성능이 우수하며,
   pymongo보다 파이썬으로 처리하는 구조인 q4(71%)로 기대보다 성능이 안좋았습니다.

결과는 3개 함수 평균 약 7초 이상 Gain 확인됩니다.
n번 반복하여 평균값 낸 것은 아니라 오차는 있습니다.

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
Index 생성 전 시간: 0.551초
Index 생성 후 시간: 0.014초
전 후 차이 시간: 0.537초
--------------------
Question3:
Index 생성 전 시간: 12.646초
Index 생성 후 시간: 0.690초
전 후 차이 시간: 11.956초
--------------------
Question4:
Index 생성 전 시간: 33.926초
Index 생성 후 시간: 24.054초
전 후 차이 시간: 9.872초
--------------------

평균 차이값: 7.455초
------------------------------------------------------------------
'''




from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client.movielens

def q1():
    # TODO: 
    client = MongoClient('mongodb://localhost:27017/')
    db = client.movielens
    import time


    # 인덱스 생성 함수
    def create_index(collection, index_spec):
        if index_spec is not None:
            collection.create_index(index_spec)

    def drop_all_indexes(db):
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            collection.drop_indexes()


    # 인덱스 정보 출력 함수
    def print_index_information(db):
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            indexes = collection.index_information()
            print(f"Indexes for {collection_name}: {indexes}")


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
        pipeline = [
            {"$match": {"movieId": {"$in": movie_ids}}},
            {"$group": {"_id": "$movieId", "avgRating": {"$avg": "$rating"}}}
        ]
        rating_aggregation = list(db.ml_ratings.aggregate(pipeline))

        # movieId별로 평균 평점을 저장할 딕셔너리 초기화
        movie_avg_ratings_dict = {rating['_id']: rating['avgRating'] for rating in rating_aggregation}

        # userId로 제한하여 해당 사용자의 영화별 평점 구하기
        user_ratings_cursor = db.ml_ratings.find({'userId': userId}, {'movieId': 1, 'rating': 1})

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
        return total_bias / num_rated_movies if num_rated_movies > 0 else 0






    # 1. 인덱스 삭제
    drop_all_indexes(db)

    # 2. 인덱스 삭제 전 인덱스 정보 출력
    print("Index 정보 (삭제 후):")
    print_index_information(db)
    print(end='\n')
        
    input_tag = "Gael Garcia Bernal"
    input_title = "Interstellar (2014)"
    input_userid = 121120

    before_time = []
    # 3. 코드 실행 결과 출력 및 생성 전 시간 저장
    results_pre = {}
    for query_name, query_func, param in [('q2', q2, input_tag), ('q3', q3, input_title), ('q4', q4, input_userid)]:
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
    print(end='\n')

    # 4. 인덱스 생성
    create_index(db.ml_tags, [('tag', 1)])
    create_index(db.ml_movies, [('movieId', 1), ('title', 1)])
    create_index(db.ml_ratings, [('movieId', 1), ('userId', 1)])

    # 5. 인덱스 삭제 전 인덱스 정보 출력
    print("Index 정보 (생성 후):")
    print_index_information(db)    
    print(end='\n')

    # 6. 코드 실행 결과 출력 및 생성 후 시간 저장
    results_post = {}
    
    after_time = []
    for query_name, query_func, param in [('q2', q2, input_tag), ('q3', q3, input_title), ('q4', q4, input_userid)]:
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


if __name__ == '__main__':
    q1()

