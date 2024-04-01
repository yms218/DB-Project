



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

    # movieId별로 평균 평점을 저장할 딕셔너리 초기화
    movie_avg_ratings_dict = {}

    # 평균 평점을 계산하기 위한 aggregate 파이프라인 정의
    pipeline = [
        {"$match": {"movieId": {"$in": movie_ids}}},
        {"$group": {"_id": "$movieId", "avgRating": {"$avg": "$rating"}}}
    ]

    # aggregate를 사용하여 평균 평점 계산
    rating_aggregation = list(db.ml_ratings.aggregate(pipeline))

    # movieId별로 평균 평점을 딕셔너리에 저장
    for rating in rating_aggregation:
        movie_avg_ratings_dict[rating['_id']] = rating['avgRating']

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










def q1():

    # 1. 인덱스 삭제
    drop_all_indexes(db)

    # 2. 인덱스 삭제 전 인덱스 정보 출력
    print("Index 정보 (삭제 후):")
    print_index_information(db)
    print(end='\n')
        
    input_tag = "Gael Garcia Bernal"
    input_title = "Interstellar (2014)"
    input_userid = 54313

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



# 예시 실행
q1()


