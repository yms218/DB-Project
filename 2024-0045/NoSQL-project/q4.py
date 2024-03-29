from sys import argv
from pymongo import MongoClient

client = MongoClient()
db = client.movielens

def q4(input_userid: int):
    # TODO:

    bias =
    print('{:.3f}'.format(bias))

if __name__ == '__main__':
    q4(int(argv[1]))

