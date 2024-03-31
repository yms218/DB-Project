from sys import argv
from pymongo import MongoClient

client = MongoClient()
db = client.movielens

def q3(input_title: str):
    # TODO: 
    movid_cursor = db.movies.find_one({'title': input_title}, {'movieId': 1, '_id':0})    
    if not movid_cursor:
        print(f"No movie found with title: {input_title}")
        return        
    mov_id = movid_cursor['movieId'] 
    
    
    ratings_cursor = db.ratings.find({ 'movieId': mov_id}, {"_id": 0, "rating": 1})
    
    sum_rating = 0
    count_rating = 0 
    
    for rating in ratings_cursor:
        sum_rating += rating['rating']
        count_rating += 1
    
    if not count_rating:
        print(f"No ratings found for movie: {input_title}")
        return                 

    _avg = sum_rating/count_rating
    print('{:.3f}'.format(_avg))


q3('Doom Generation, The (1995)')
# if __name__ == '__main__':
    # q3(argv[1])

