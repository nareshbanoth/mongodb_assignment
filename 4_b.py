from pymongo import MongoClient


def task_one(n, movies):
    pipeline = [
        {'$project': {'title': '$title', 'rating': '$imdb.rating'}},
        {'$match': {'rating': {'$exists': True, '$ne': ''}}},
        {'$group': {'_id': {'rating': '$rating', 'title': '$title'}}},
        {'$sort': {'_id.rating': -1}},
        {'$limit': n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_two(n, year, movies):
    pipeline = [
        {"$match": {"year": year}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_three(n, movies):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": "1000"}}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_four(n, movies, pattern):
    pipeline = [
        {"$match": {"title": {"$regex": pattern}}},
        {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_five(n, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"director_name": "$directors"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_six(n, year, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.directors": 1, "no_of_films": 1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_seven(n, genres, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_eight(n, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_nine(n, year, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.year": 0}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_ten(n, genres, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$project": {"_id.genres": 0}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_eleven(n, movies, genres):
    pipeline = [
        {"$unwind": "$genres"},
        {"$match": {"genres": genres, 'imdb.rating': {'$exists': True, '$ne': ''}}},
        {"$project": {"_id": 0, "title": 1, 'rating': '$imdb.rating'}},
        {'$group': {'_id': {'title': '$title', 'rating': '$rating'}}},
        {"$sort": {"_id.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res

def queries(movies):
    n = int(input("Enter the value for N: "))
    taskOne = task_one(n, movies)
    print(taskOne)

    year = int(input("Enter the year: "))
    taskTwo = task_two(n, year, movies)
    print(taskTwo)

    print("Top n movies with vote greater than 1000")
    taskThree = task_three(n, movies)
    print(taskThree)

    print("Top n movies with movies title matching: ")
    taskFour = task_four(n, movies, "The")
    print(taskFour)

    print('Top n directors with maximum no. of votes: ')
    taskFive = task_five(n, movies)
    print(taskFive)

    print('Top directors who created maximum movies on given year')
    year = int(input("enter year: "))
    taskSix = task_six(n, year, movies)
    print(taskSix)

    print("top directors in given genre")
    taskSeven = task_seven(n, "Action", movies)
    print(taskSeven)

    print("top `n actors who starred in the maximum number of movies")
    taskEight = task_eight(n, movies)
    print(taskEight)

    print("top n actors who starred in the maximum number of movies in a given year")
    year = int(input("enter the year: "))
    taskNine = task_nine(n,  year, movies)
    print(taskNine)

    print("top n actors who starred in the maximum number of movies for a given genre")
    taskTen = task_ten(n, "Action", movies)
    print(taskTen)

    print("top n movies for each genre with the highest IMDB rating")
    taskEleven = task_eleven(n, movies, "Action")
    print(taskEleven)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")

    # collection
    movies = client.entertainment.movies

    queries(movies)
