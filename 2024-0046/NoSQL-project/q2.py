from sys import argv
from pymongo import MongoClient

client = MongoClient()
db = client.movielens



def q2(input_tag: str):
    # TODO: 
    mov_cursor = db.tags.find({'tag': input_tag}, {'movieId': 1, '_id': 0})
    mov_list = set([m['movieId'] for m in mov_cursor])
    if not mov_list:
        print(f"No title found with tag: {input_tag}")
        return          
    titles = set()
    for mov in mov_list:
        title = db.movies.find_one({ 'movieId': mov}, {'title':1, '_id':0})
        titles.add(title['title'])
  
    for title in sorted(titles):
        print(title)
        

if __name__ == '__main__':
    q2(argv[1])

