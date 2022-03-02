import pymongo
from pprint import pprint
from pymongo import MongoClient

# QUESTION 1
client = MongoClient("mongodb://localhost:27017")

# QUESTION 3
movies = client.entertainment.movies
comments = client.entertainment.comments
theaters = client.entertainment.theaters
users = client.entertainment.users

new_movie = {
    "plot": "Days of our lives",
    "genres": ["dark", "thriller"],
    "title": "title"
}

new_comment = {
    "name": "Joe Tribbiani",
    "text": "How you doin?",
}

new_theater = {
    "theater_id": 69,
    "location": {
        "address": {
            "city": "Central city"
        }
    }
}

new_user = {
    "name": "Dr Drake Ramore",
    "email": "drdrakeramore@friends.com",
    "password": "thereforyou"
}

# Insertion
new_movie_id = movies.insert_one(new_movie).inserted_id
new_comment_id = comments.insert_one(new_comment).inserted_id
new_theater_id = theaters.insert_one(new_theater).inserted_id
new_user_id = users.insert_one(new_user).inserted_id

# Fetching new documents
movie = movies.find_one({"_id": new_movie_id})
comment = comments.find_one({"_id": new_comment_id})
theater = theaters.find_one({"_id": new_theater_id})
user = users.find_one({"_id": new_user_id})

# Printing newly added documents
pprint(movie)
print(movie['plot'])
pprint(comment['text'])
pprint(theater['location']['address']['city'])
pprint(user['email'])
