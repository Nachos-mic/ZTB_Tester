import pytest
import pymongo
import random
import string
import time
from datetime import datetime, date
from bson import ObjectId
from pymongo import MongoClient


@pytest.fixture(scope="module")
def db():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    database = client["ZTB_Database_Mongo"]
    yield database
    client.close()

def random_suffix(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def random_isbn():
    return int(''.join(random.choices(string.digits, k=10)))

def random_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

# -----------------------
# POST tests (CREATE)
# -----------------------

def test_insert_book_genre(db):
    genre_name = f"Genre-{random_suffix()}"
    pop = random.randint(1, 100)
    genre_id = random.randint(1000000, 9999999)
    
    result = db.genres.insert_one({
        "id": genre_id,
        "genre": genre_name,
        "popularity": pop
    })
    
    assert result.inserted_id is not None
    doc = db.genres.find_one({"_id": result.inserted_id})
    assert doc["genre"] == genre_name
    assert doc["popularity"] == pop

def test_insert_user(db):
    loc = f"City-{random_suffix()}"
    age = random.randint(18, 80)
    user_id = random.randint(1000000, 9999999)
    
    result = db.users.insert_one({
        "users_id": user_id,
        "location": loc,
        "age": str(age)
    })
    
    assert result.inserted_id is not None
    doc = db.users.find_one({"_id": result.inserted_id})
    assert doc["location"] == loc
    assert int(doc["age"]) == age

def test_insert_publisher_and_author(db):
    pub_id = random_code()
    auth_id = random_code()
    name = f"Pub-{random_suffix()}"
    
    pub_result = db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": name,
        "address": "Addr",
        "country": "PL",
        "mail": f"{name}@mail.com",
        "phone": "123456"
    })
    
    auth_result = db.authors.insert_one({
        "author_id": auth_id,
        "author_name": f"Auth-{random_suffix()}",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    assert pub_result.inserted_id is not None
    assert auth_result.inserted_id is not None
    
    pub_doc = db.publishers.find_one({"publisher_id": pub_id})
    auth_doc = db.authors.find_one({"author_id": auth_id})
    assert pub_doc is not None
    assert auth_doc is not None

def test_insert_order_and_return(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    user_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)
    return_id = random.randint(1000000, 9999999)

    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "T",
        "year_of_release": 2025,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.users.insert_one({
        "users_id": user_id,
        "location": "L",
        "age": "30"
    })
    
    order_result = db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "price": 99.9
    })
    
    return_result = db.returns.insert_one({
        "return_id": return_id,
        "order_id": order_id,
        "return_date": datetime.now().strftime("%Y-%m-%d"),
        "reason_description": "no reason"
    })
    
    assert order_result.inserted_id is not None
    assert return_result.inserted_id is not None

def test_insert_book_rating_aggregation(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)

    db.users.insert_one({
        "users_id": user_id,
        "location": "X",
        "age": "40"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "BR",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })

    for rating in [3, 4, 5]:
        db.ratings.insert_one({
            "user_id": user_id,
            "isbn": isbn,
            "book_rating": rating
        })

    pipeline = [
        {"$match": {"isbn": isbn}},
        {"$group": {"_id": "$isbn", "count": {"$sum": 1}}}
    ]
    result = list(db.ratings.aggregate(pipeline))
    assert len(result) > 0 and result[0]["count"] == 3

def test_insert_book_rating_with_lookup(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)


    db.users.insert_one({
        "users_id": user_id,
        "location": "Y",
        "age": "50"
    })

    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })

    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })

    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })

    db.books.insert_one({
        "isbn": isbn,
        "book_name": "BRJ",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })

    rating = random.randint(1, 5)
    db.ratings.insert_one({
        "user_id": user_id,
        "isbn": isbn,
        "book_rating": rating
    })

    pipeline = [
        {"$match": {"user_id": user_id, "isbn": isbn}},
        {"$lookup": {
            "from": "books",
            "localField": "isbn",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$project": {
            "book_rating": 1,
            "book_name": "$book_info.book_name"
        }}
    ]

    result = list(db.ratings.aggregate(pipeline))
    assert len(result) > 0
    assert result[0]["book_rating"] == rating
    assert result[0]["book_name"] == "BRJ"

def test_get_order_with_book_and_author_lookup(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    user_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)

    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "GA",
        "country_of_origin": "FR",
        "birth_date": "1985-05-05"
    })

    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "PB",
        "address": "A",
        "country": "DE",
        "mail": "pb@mail",
        "phone": "000000"
    })

    db.genres.insert_one({
        "id": genre_id,
        "genre": "G2",
        "popularity": 15
    })

    db.books.insert_one({
        "isbn": isbn,
        "book_name": "Name",
        "year_of_release": 2023,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })

    db.users.insert_one({
        "users_id": user_id,
        "location": "Loc",
        "age": "22"
    })

    db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "price": 55.5
    })

    pipeline = [
        {"$match": {"order_id": order_id}},
        {"$lookup": {
            "from": "books",
            "localField": "isbn",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$lookup": {
            "from": "authors",
            "localField": "book_info.author_id",
            "foreignField": "author_id",
            "as": "author_info"
        }},
        {"$unwind": "$author_info"},
        {"$project": {
            "order_id": 1,
            "book_name": "$book_info.book_name",
            "author_name": "$author_info.author_name"
        }}
    ]

    result = list(db.orders.aggregate(pipeline))
    assert len(result) > 0
    assert result[0]["order_id"] == order_id
    assert result[0]["book_name"] == "Name"
    assert result[0]["author_name"] == "GA"

def test_get_average_book_rating_above_aggregation(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)


    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })

    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })

    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })

    db.books.insert_one({
        "isbn": isbn,
        "book_name": "Avg",
        "year_of_release": 2022,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })


    for i, rating in enumerate([3, 5, 4]):
        user_id = random.randint(1000000, 9999999) + i
        db.users.insert_one({
            "users_id": user_id,
            "location": "U",
            "age": "30"
        })
        db.ratings.insert_one({
            "user_id": user_id,
            "isbn": isbn,
            "book_rating": rating
        })

    pipeline = [
        {"$group": {
            "_id": "$isbn",
            "avg_rating": {"$avg": "$book_rating"}
        }},
        {"$match": {"avg_rating": {"$gt": 3.5}}},
        {"$lookup": {
            "from": "books",
            "localField": "_id",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$project": {"isbn": "$_id"}}
    ]

    result = list(db.ratings.aggregate(pipeline))
    found_isbns = [doc["isbn"] for doc in result]
    assert isbn in found_isbns

def test_insert_book_rating_lookup(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    

    db.users.insert_one({
        "users_id": user_id,
        "location": "Y",
        "age": "50"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "BRJ",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    rating = random.randint(1, 5)
    db.ratings.insert_one({
        "user_id": user_id,
        "isbn": isbn,
        "book_rating": rating
    })

    pipeline = [
        {"$match": {"user_id": user_id, "isbn": isbn}},
        {"$lookup": {
            "from": "books",
            "localField": "isbn",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$project": {
            "book_rating": 1,
            "book_name": "$book_info.book_name"
        }}
    ]
    
    result = list(db.ratings.aggregate(pipeline))
    assert len(result) > 0
    assert result[0]["book_rating"] == rating
    assert result[0]["book_name"] == "BRJ"

# -----------------------
# GET tests (READ)
# -----------------------

def test_get_order_with_book_and_author(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    user_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)

    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "GA",
        "country_of_origin": "FR",
        "birth_date": "1985-05-05"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "PB",
        "address": "A",
        "country": "DE",
        "mail": "pb@mail",
        "phone": "000000"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "G2",
        "popularity": 15
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "Name",
        "year_of_release": 2023,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.users.insert_one({
        "users_id": user_id,
        "location": "Loc",
        "age": "22"
    })
    
    db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "price": 55.5
    })
    

    pipeline = [
        {"$match": {"order_id": order_id}},
        {"$lookup": {
            "from": "books",
            "localField": "isbn",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$lookup": {
            "from": "authors",
            "localField": "book_info.author_id",
            "foreignField": "author_id",
            "as": "author_info"
        }},
        {"$unwind": "$author_info"},
        {"$project": {
            "order_id": 1,
            "book_name": "$book_info.book_name",
            "author_name": "$author_info.author_name"
        }}
    ]
    
    result = list(db.orders.aggregate(pipeline))
    assert len(result) > 0
    assert result[0]["order_id"] == order_id
    assert result[0]["book_name"] == "Name"
    assert result[0]["author_name"] == "GA"

def test_get_average_book_rating_above(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)

    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "P",
        "address": "A",
        "country": "PL",
        "mail": "p@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "A",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "G",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "Avg",
        "year_of_release": 2022,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })

    for i, rating in enumerate([3, 5, 4]):
        user_id = random.randint(1000000, 9999999) + i
        db.users.insert_one({
            "users_id": user_id,
            "location": "U",
            "age": "30"
        })
        db.ratings.insert_one({
            "user_id": user_id,
            "isbn": isbn,
            "book_rating": rating
        })

    pipeline = [
        {"$group": {
            "_id": "$isbn",
            "avg_rating": {"$avg": "$book_rating"}
        }},
        {"$match": {"avg_rating": {"$gt": 3.5}}},
        {"$lookup": {
            "from": "books",
            "localField": "_id",
            "foreignField": "isbn",
            "as": "book_info"
        }},
        {"$unwind": "$book_info"},
        {"$project": {"isbn": "$_id"}}
    ]
    
    result = list(db.ratings.aggregate(pipeline))
    found_isbns = [doc["isbn"] for doc in result]
    assert isbn in found_isbns

def test_get_genre_book_counts_aggregation(db):
    pub_id = get_any_publisher_id(db)
    auth_id = get_any_author_id(db)
    
    genre1_id = random.randint(1000000, 9999999)
    genre2_id = random.randint(1000000, 9999999)
    
    db.genres.insert_one({
        "id": genre1_id,
        "genre": "G3",
        "popularity": 5
    })
    
    db.genres.insert_one({
        "id": genre2_id,
        "genre": "G4",
        "popularity": 8
    })
    

    for i in range(2):
        db.books.insert_one({
            "isbn": random_isbn(),
            "book_name": "X",
            "year_of_release": 2021,
            "genre_id": genre1_id,
            "publisher_id": pub_id,
            "author_id": auth_id
        })
    

    db.books.insert_one({
        "isbn": random_isbn(),
        "book_name": "Y",
        "year_of_release": 2020,
        "genre_id": genre2_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })

    pipeline = [
        {"$match": {"id": {"$in": [genre1_id, genre2_id]}}},
        {"$lookup": {
            "from": "books",
            "localField": "id",
            "foreignField": "genre_id",
            "as": "books"
        }},
        {"$project": {
            "genre": 1,
            "book_count": {"$size": "$books"}
        }}
    ]
    
    result = list(db.genres.aggregate(pipeline))
    result_dict = {doc["genre"]: doc["book_count"] for doc in result}
    assert result_dict["G3"] == 2
    assert result_dict["G4"] == 1

def test_get_users_and_orders_lookup(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)
    

    db.users.insert_one({
        "users_id": user_id,
        "location": "JoinLoc",
        "age": "35"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "JP",
        "address": "A",
        "country": "PL",
        "mail": "jp@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "JA",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "JG",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "JoinBook",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "price": 88.8
    })

    pipeline = [
        {"$match": {"users_id": user_id}},
        {"$lookup": {
            "from": "orders",
            "localField": "users_id",
            "foreignField": "user_id",
            "as": "orders"
        }},
        {"$unwind": "$orders"},
        {"$project": {
            "location": 1,
            "order_cost": "$orders.price"
        }}
    ]
    
    result = list(db.users.aggregate(pipeline))
    assert len(result) > 0
    assert result[0]["location"] == "JoinLoc"
    assert result[0]["order_cost"] == 88.8

def get_any_publisher_id(db):
    doc = db.publishers.find_one()
    if doc:
        return doc["publisher_id"]
    pub_id = random_code()
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "DefaultPub",
        "address": "Addr",
        "country": "PL",
        "mail": "default@mail.com",
        "phone": "000000"
    })
    return pub_id

def get_any_author_id(db):
    doc = db.authors.find_one()
    if doc:
        return doc["author_id"]
    auth_id = random_code()
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "DefaultAuthor",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    return auth_id

# -----------------------
# PUT tests (UPDATE)
# -----------------------

def test_update_genre_popularity(db):
    genre_id = random.randint(1000000, 9999999)
    db.genres.insert_one({
        "id": genre_id,
        "genre": "UG",
        "popularity": 1
    })
    
    result = db.genres.update_one(
        {"id": genre_id},
        {"$inc": {"popularity": 5}}
    )
    
    assert result.modified_count == 1
    doc = db.genres.find_one({"id": genre_id})
    assert doc["popularity"] == 6

def test_update_user_location(db):
    user_id = random.randint(1000000, 9999999)
    db.users.insert_one({
        "users_id": user_id,
        "location": "OldLoc",
        "age": "30"
    })
    
    new_loc = f"Loc-{random_suffix()}"
    result = db.users.update_one(
        {"users_id": user_id},
        {"$set": {"location": new_loc}}
    )
    
    assert result.modified_count == 1
    doc = db.users.find_one({"users_id": user_id})
    assert doc["location"] == new_loc

def test_update_genre_popularity_aggregation(db):
    genre_id = random.randint(1000000, 9999999)
    pub_id = get_any_publisher_id(db)
    auth_id = get_any_author_id(db)
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "UGG",
        "popularity": 1
    })

    for i in range(2):
        db.books.insert_one({
            "isbn": random_isbn(),
            "book_name": "UGG-Book",
            "year_of_release": 2021,
            "genre_id": genre_id,
            "publisher_id": pub_id,
            "author_id": auth_id
        })

    pipeline = [
        {"$group": {"_id": "$genre_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    genres_to_update = [doc["_id"] for doc in db.books.aggregate(pipeline)]
    
    if genre_id in genres_to_update:
        result = db.genres.update_one(
            {"id": genre_id},
            {"$inc": {"popularity": 10}}
        )
        assert result.modified_count == 1
        
        doc = db.genres.find_one({"id": genre_id})
        assert doc["popularity"] == 11

def test_update_user_with_order_lookup(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)

    db.users.insert_one({
        "users_id": user_id,
        "location": "OldJoinLoc",
        "age": "28"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "PUJ",
        "address": "A",
        "country": "PL",
        "mail": "puj@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "AUJ",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "GJ",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "JoinBook2",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "price": 77.7
    })
    
    new_loc = f"JoinLoc-{random_suffix()}"

    users_with_orders = db.orders.distinct("user_id")
    if user_id in users_with_orders:
        result = db.users.update_one(
            {"users_id": user_id},
            {"$set": {"location": new_loc}}
        )
        assert result.modified_count == 1
        
        doc = db.users.find_one({"users_id": user_id})
        assert doc["location"] == new_loc

# -----------------------
# DELETE tests
# -----------------------

def test_delete_genre_by_id(db):
    genre_id = random.randint(1000000, 9999999)
    db.genres.insert_one({
        "id": genre_id,
        "genre": "DG",
        "popularity": 3
    })
    
    result = db.genres.delete_one({"id": genre_id})
    assert result.deleted_count == 1
    
    doc = db.genres.find_one({"id": genre_id})
    assert doc is None

def test_delete_user_by_id(db):
    user_id = random.randint(1000000, 9999999)
    db.users.insert_one({
        "users_id": user_id,
        "location": "DU",
        "age": "25"
    })
    
    result = db.users.delete_one({"users_id": user_id})
    assert result.deleted_count == 1
    
    doc = db.users.find_one({"users_id": user_id})
    assert doc is None

def test_delete_books_with_few_ratings_aggregation(db):
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    user_id = random.randint(1000000, 9999999)

    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "DP",
        "address": "A",
        "country": "PL",
        "mail": "dp@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "DA",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "DG",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "DelBook",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.users.insert_one({
        "users_id": user_id,
        "location": "DelLoc",
        "age": "33"
    })
    
    db.ratings.insert_one({
        "user_id": user_id,
        "isbn": isbn,
        "book_rating": 5
    })

    pipeline = [
        {"$group": {"_id": "$isbn", "count": {"$sum": 1}}},
        {"$match": {"count": {"$lt": 2}}}
    ]
    
    books_to_delete = [doc["_id"] for doc in db.ratings.aggregate(pipeline)]

    if books_to_delete:
        result = db.books.delete_many({"isbn": {"$in": books_to_delete}})
        assert result.deleted_count > 0

    doc = db.books.find_one({"isbn": isbn})
    assert doc is None

def test_delete_orders_with_user_lookup(db):
    user_id = random.randint(1000000, 9999999)
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = random.randint(1000000, 9999999)
    order_id = random.randint(1000000, 9999999)

    db.users.insert_one({
        "users_id": user_id,
        "location": "DelJoinLoc",
        "age": "45"
    })
    
    db.publishers.insert_one({
        "publisher_id": pub_id,
        "name": "DPJ",
        "address": "A",
        "country": "PL",
        "mail": "dpj@mail",
        "phone": "000000"
    })
    
    db.authors.insert_one({
        "author_id": auth_id,
        "author_name": "DAJ",
        "country_of_origin": "US",
        "birth_date": "1970-01-01"
    })
    
    db.genres.insert_one({
        "id": genre_id,
        "genre": "DGJ",
        "popularity": 1
    })
    
    db.books.insert_one({
        "isbn": isbn,
        "book_name": "DelJoinBook",
        "year_of_release": 2024,
        "genre_id": genre_id,
        "publisher_id": pub_id,
        "author_id": auth_id
    })
    
    db.orders.insert_one({
        "order_id": order_id,
        "isbn": isbn,
        "user_id": user_id,
        "order_date": "2024-01-01",
        "order_cost": 66.6
    })

    users_to_delete = db.users.find({"location": "DelJoinLoc"})
    user_ids_to_delete = [user["users_id"] for user in users_to_delete]

    if user_ids_to_delete:
        result = db.orders.delete_many({"user_id": {"$in": user_ids_to_delete}})
        assert result.deleted_count > 0

    doc = db.orders.find_one({"user_id": user_id})
    assert doc is None

# -----------------------
# TEST START
# -----------------------

def mongo_tests():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['ZTB_Database_Mongo']

    tests = [
        test_insert_book_genre,
        test_insert_user,
        test_insert_publisher_and_author,
        test_insert_order_and_return,
        test_insert_book_rating_aggregation,
        test_insert_book_rating_with_lookup,
        test_get_order_with_book_and_author_lookup,
        test_get_average_book_rating_above_aggregation,
        test_get_genre_book_counts_aggregation,
        test_get_users_and_orders_lookup,
        test_update_genre_popularity,
        test_update_user_location,
        test_update_genre_popularity_aggregation,
        test_update_user_with_order_lookup,
        test_delete_genre_by_id,
        test_delete_user_by_id,
        test_delete_books_with_few_ratings_aggregation,
        test_delete_orders_with_user_lookup,
    ]

    crud_categories = {
        'CREATE': [
            'test_insert_book_genre',
            'test_insert_user',
            'test_insert_publisher_and_author',
            'test_insert_order_and_return',
            'test_insert_book_rating_aggregation',
            'test_insert_book_rating_with_lookup'
        ],
        'READ': [
            'test_get_order_with_book_and_author_lookup',
            'test_get_average_book_rating_above_aggregation',
            'test_get_genre_book_counts_aggregation',
            'test_get_users_and_orders_lookup'
        ],
        'UPDATE': [
            'test_update_genre_popularity',
            'test_update_user_location',
            'test_update_genre_popularity_aggregation',
            'test_update_user_with_order_lookup'
        ],
        'DELETE': [
            'test_delete_genre_by_id',
            'test_delete_user_by_id',
            'test_delete_books_with_few_ratings_aggregation',
            'test_delete_orders_with_user_lookup'
        ]
    }

    data_sizes = [500000]
    runs_per_test = 5

    print(f"Znaleziono {len(tests)} testów MongoDB")

    total_start = time.time()
    individual_results = {}
    crud_results = {}

    for data_size in data_sizes:
        print(f"\n{'=' * 60}")
        print(f"TESTY MONGODB DLA ROZMIARU DANYCH: {data_size}")
        print(f"{'=' * 60}")

        individual_results[data_size] = {}
        crud_results[data_size] = {
            'CREATE': [],
            'READ': [],
            'UPDATE': [],
            'DELETE': []
        }

        prepare_mongo_test_data(db, data_size)

        for test in tests:
            times = []
            status = "OK"
            for run in range(runs_per_test):
                start = time.time()
                try:
                    test(db)
                    duration = time.time() - start
                    times.append(duration)
                except AssertionError as e:
                    status = f"FAIL ({e})"
                    break
                except Exception as e:
                    status = f"ERROR ({e.__class__.__name__})"
                    break

            if times:
                avg_time = sum(times) / len(times)

                test_name = test.__name__

                individual_results[data_size][test_name] = avg_time

                for crud_op, test_names in crud_categories.items():
                    if test_name in test_names:
                        crud_results[data_size][crud_op].append(avg_time)
                        break
            else:
                print(f"{test.__name__:35} → {status:10} (test nie przeszedł)")

        cleanup_mongo_test_data(db)

    final_results = {}
    for data_size in data_sizes:
        final_results[data_size] = {}
        for crud_op in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
            times = crud_results[data_size][crud_op]
            if times:
                final_results[data_size][crud_op] = sum(times)
            else:
                final_results[data_size][crud_op] = 0.0

        for test_name, time_val in individual_results[data_size].items():
            final_results[data_size][test_name] = time_val

    client.close()
    return final_results


def prepare_mongo_test_data(db, size):
    try:
        cleanup_mongo_test_data(db)
        print(f"Przygotowywanie {size} rekordów danych testowych MongoDB...")

        genres_data = []
        for i in range(20):
            genre_data = {
                "id": 1000000 + i,
                "genre": f"TestGenre_{i}",
                "popularity": i % 10 + 1
            }
            genres_data.append(genre_data)
        db.genres.insert_many(genres_data)

        publishers_data = []
        for i in range(50):
            publisher_data = {
                "publisher_id": f"TESTPUB{i:04d}",
                "name": f"TestPublisher_{i}",
                "address": f"TestAddress_{i}",
                "country": f"TestCountry_{i % 10}",
                "mail": f"publisher{i}@test.com",
                "phone": f"123-456-{i:04d}"
            }
            publishers_data.append(publisher_data)
        db.publishers.insert_many(publishers_data)

        authors_data = []
        for i in range(100):
            author_data = {
                "author_id": f"TESTAUTH{i:04d}",
                "author_name": f"TestAuthor_{i}",
                "country_of_origin": f"TestCountry_{i % 20}",
                "birth_date": f"19{50 + i % 50}-01-01"
            }
            authors_data.append(author_data)
        db.authors.insert_many(authors_data)

        genre_ids = [doc["id"] for doc in db.genres.find({"genre": {"$regex": "^TestGenre_"}})]
        publisher_ids = [doc["publisher_id"] for doc in db.publishers.find({"name": {"$regex": "^TestPublisher_"}})]
        author_ids = [doc["author_id"] for doc in db.authors.find({"author_name": {"$regex": "^TestAuthor_"}})]

        users_data = []
        for i in range(size):
            user_data = {
                "users_id": 2000000 + i,
                "location": f"TestCity_{i % 100}",
                "age": str(18 + (i % 62))
            }
            users_data.append(user_data)

        batch_size = 1000
        for i in range(0, len(users_data), batch_size):
            batch = users_data[i:i + batch_size]
            db.users.insert_many(batch)

        books_data = []
        for i in range(size):
            book_data = {
                "isbn": f"TEST-{i:010d}",
                "book_name": f"TestBook_{i}",
                "year_of_release": 1950 + (i % 74),
                "genre_id": genre_ids[i % len(genre_ids)],
                "publisher_id": publisher_ids[i % len(publisher_ids)],
                "author_id": author_ids[i % len(author_ids)]
            }
            books_data.append(book_data)

        for i in range(0, len(books_data), batch_size):
            batch = books_data[i:i + batch_size]
            db.books.insert_many(batch)

        user_ids = [doc["users_id"] for doc in db.users.find({"location": {"$regex": "^TestCity_"}})]
        book_isbns = [doc["isbn"] for doc in db.books.find({"book_name": {"$regex": "^TestBook_"}})]

        ratings_data = []
        for i in range(min(size, len(user_ids) * len(book_isbns) // 10)):
            rating_data = {
                "user_id": user_ids[i % len(user_ids)],
                "isbn": book_isbns[i % len(book_isbns)],
                "book_rating": (i % 5) + 1
            }
            ratings_data.append(rating_data)
        
        if ratings_data:
            for i in range(0, len(ratings_data), batch_size):
                batch = ratings_data[i:i + batch_size]
                try:
                    db.ratings.insert_many(batch, ordered=False)
                except Exception:
                    pass

        orders_data = []
        for i in range(size // 2):
            order_data = {
                "order_id": 3000000 + i,
                "isbn": book_isbns[i % len(book_isbns)],
                "user_id": user_ids[i % len(user_ids)],
                "order_date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "order_cost": round(10.0 + (i % 50), 2)
            }
            orders_data.append(order_data)
        
        if orders_data:
            for i in range(0, len(orders_data), batch_size):
                batch = orders_data[i:i + batch_size]
                db.orders.insert_many(batch)

        print(f"Przygotowano dane testowe MongoDB o rozmiarze {size}")

    except Exception as e:
        print(f"Błąd podczas przygotowywania danych MongoDB: {e}")


def cleanup_mongo_test_data(db):
    try:
        db.orders.delete_many({"user_id": {"$gte": 2000000}})
        db.ratings.delete_many({"user_id": {"$gte": 2000000}})
        db.books.delete_many({"book_name": {"$regex": "^TestBook_"}})
        db.users.delete_many({"location": {"$regex": "^TestCity_"}})
        db.authors.delete_many({"author_name": {"$regex": "^TestAuthor_"}})
        db.publishers.delete_many({"name": {"$regex": "^TestPublisher_"}})
        db.genres.delete_many({"genre": {"$regex": "^TestGenre_"}})
        print("Dane testowe MongoDB zostały wyczyszczone")
    except Exception as e:
        print(f"Błąd podczas czyszczenia danych MongoDB: {e}")


def get_any_mongo(db, collection_name, field_name):
    doc = db[collection_name].find_one()
    if doc and field_name in doc:
        return doc[field_name]
    raise Exception(f"No data in {collection_name}")


if __name__ == "__main__":
    mongo_tests()