import pytest
import boto3
import random
import string
import time
from datetime import datetime
from decimal import Decimal

@pytest.fixture(scope="module")
def db():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='eu-north-1',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    yield dynamodb

def random_suffix(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def random_isbn():
    return ''.join(random.choices(string.digits, k=10))

def random_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def test_insert_book_genre(db):
    table = db.Table('Book_Genres')
    genre_id = str(random.randint(1000000, 9999999))
    genre_name = f"Genre-{random_suffix()}"
    popularity = Decimal(random.randint(1, 100))
    table.put_item(Item={
        'id': genre_id,
        'genre_name': genre_name,
        'popularity': popularity
    })
    result = table.get_item(Key={'id': genre_id})
    assert result['Item']['genre_name'] == genre_name
    assert result['Item']['popularity'] == popularity

def test_insert_user(db):
    table = db.Table('Users')
    user_id = str(random.randint(1000000, 9999999))
    location = f"City-{random_suffix()}"
    age = Decimal(random.randint(18, 80))
    table.put_item(Item={
        'users_id': user_id,
        'location': location,
        'age': age
    })
    result = table.get_item(Key={'users_id': user_id})
    assert result['Item']['location'] == location
    assert result['Item']['age'] == age

def test_insert_publisher_and_author(db):
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    pub_id = random_code()
    auth_id = random_code()
    name = f"Pub-{random_suffix()}"
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': name,
        'address': "Addr",
        'country': "PL",
        'email': f"{name}@mail.com",
        'phone': "123456"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': f"Auth-{random_suffix()}",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    pub_doc = pub_table.get_item(Key={'publisher_id': pub_id})
    auth_doc = auth_table.get_item(Key={'author_id': auth_id})
    assert pub_doc.get('Item') is not None
    assert auth_doc.get('Item') is not None

def test_insert_order_and_return(db):
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    user_table = db.Table('Users')
    order_table = db.Table('Orders')
    return_table = db.Table('Returns')
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    user_id = str(random.randint(1000000, 9999999))
    order_id = str(random.randint(1000000, 9999999))
    return_id = str(random.randint(1000000, 9999999))
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "P",
        'address': "A",
        'country': "PL",
        'email': "p@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "A",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "G",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "T",
        'Year_Of_Release': Decimal(2025),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "L",
        'age': Decimal(30)
    })
    order_table.put_item(Item={
        'Order_ID': order_id,
        'ISBN': isbn,
        'User_ID': user_id,
        'Order_Date': datetime.now().strftime("%Y-%m-%d"),
        'Order_Cost': Decimal('99.9')
    })
    return_table.put_item(Item={
        'Return_ID': return_id,
        'Order_ID': order_id,
        'Return_Date': datetime.now().strftime("%Y-%m-%d"),
        'Reason_Description': "no reason"
    })
    order_doc = order_table.get_item(Key={'Order_ID': order_id})
    return_doc = return_table.get_item(Key={'Return_ID': return_id})
    assert order_doc.get('Item') is not None
    assert return_doc.get('Item') is not None

def test_insert_book_rating_group_by(db):
    user_table = db.Table('Users')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    rating_table = db.Table('Book_Ratings')
    user_id = str(random.randint(1000000, 9999999))
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    timestamp = int(time.time() * 1000)
    rating_ids = [f"{timestamp}_{i}" for i in range(3)]
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "X",
        'age': Decimal(40)
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "P",
        'address': "A",
        'country': "PL",
        'email': "p@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "A",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "G",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "BR",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    for i, rating in enumerate([3, 4, 5]):
        rating_table.put_item(Item={
            'Rating_ID': rating_ids[i],
            'Book_Rating': Decimal(rating),
            'User_ID': user_id,
            'ISBN': isbn
        })
    time.sleep(0.1)
    ratings = []
    for rating_id in rating_ids:
        try:
            response = rating_table.get_item(Key={'Rating_ID': rating_id})
            if 'Item' in response:
                item = response['Item']
                if item['ISBN'] == isbn:
                    ratings.append(item['Book_Rating'])
        except Exception as e:
            pass
    assert len(ratings) == 3

def test_insert_book_rating_join(db):
    user_table = db.Table('Users')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    rating_table = db.Table('Book_Ratings')
    user_id = str(random.randint(1000000, 9999999))
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    rating_id = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "Y",
        'age': Decimal(50)
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "P",
        'address': "A",
        'country': "PL",
        'email': "p@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "A",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "G",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "BRJ",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    rating = random.randint(1, 5)
    rating_table.put_item(Item={
        'Rating_ID': rating_id,
        'Book_Rating': Decimal(rating),
        'User_ID': user_id,
        'ISBN': isbn
    })
    time.sleep(0.1)
    rating_doc = rating_table.get_item(Key={'Rating_ID': rating_id})['Item']
    book_doc = book_table.get_item(Key={'ISBN': isbn})['Item']
    assert rating_doc['Book_Rating'] == Decimal(rating)
    assert book_doc['Book_Name'] == "BRJ"

def test_get_order_with_book_and_author(db):
    order_table = db.Table('Orders')
    book_table = db.Table('Books')
    author_table = db.Table('Authors')
    pub_table = db.Table('Publishers')
    genre_table = db.Table('Book_Genres')
    user_table = db.Table('Users')
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    user_id = str(random.randint(1000000, 9999999))
    order_id = str(random.randint(1000000, 9999999))
    author_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "GA",
        'country_of_origin': "FR",
        'birth_date': "1985-05-05"
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "PB",
        'address': "A",
        'country': "DE",
        'email': "pb@mail",
        'phone': "000000"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "G2",
        'popularity': Decimal(15)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "Name",
        'Year_Of_Release': Decimal(2023),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "Loc",
        'age': Decimal(22)
    })
    order_table.put_item(Item={
        'Order_ID': order_id,
        'ISBN': isbn,
        'User_ID': user_id,
        'Order_Date': datetime.now().strftime("%Y-%m-%d"),
        'Order_Cost': Decimal('55.5')
    })
    order = order_table.get_item(Key={'Order_ID': order_id})['Item']
    book = book_table.get_item(Key={'ISBN': order['ISBN']})['Item']
    author = author_table.get_item(Key={'author_id': book['Author_id']})['Item']
    assert order['Order_ID'] == order_id
    assert book['Book_Name'] == "Name"
    assert author['author_name'] == "GA"

def test_get_average_book_rating_above(db):
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    user_table = db.Table('Users')
    rating_table = db.Table('Book_Ratings')
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    timestamp = int(time.time() * 1000)
    rating_ids = [f"{timestamp}_{i}" for i in range(3)]
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "P",
        'address': "A",
        'country': "PL",
        'email': "p@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "A",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "G",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "Avg",
        'Year_Of_Release': Decimal(2022),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    vals = []
    for i, r in enumerate([3, 5, 4]):
        user_id = str(random.randint(1000000, 9999999))
        user_table.put_item(Item={
            'users_id': user_id,
            'location': "U",
            'age': Decimal(30)
        })
        rating_table.put_item(Item={
            'Rating_ID': rating_ids[i],
            'Book_Rating': Decimal(r),
            'User_ID': user_id,
            'ISBN': isbn
        })
        vals.append(r)
    time.sleep(0.1)
    ratings = []
    for rating_id in rating_ids:
        try:
            response = rating_table.get_item(Key={'Rating_ID': rating_id})
            if 'Item' in response:
                item = response['Item']
                if item['ISBN'] == isbn:
                    ratings.append(float(item['Book_Rating']))
        except Exception as e:
            pass
    if len(ratings) > 0:
        avg = sum(ratings) / len(ratings)
    else:
        avg = 0
    assert avg > 3.5, f"Średnia ocena {avg} nie jest większa niż 3.5"
    assert len(ratings) == 3, f"Oczekiwano 3 oceny, znaleziono {len(ratings)}"

def test_get_genre_book_counts_group_by(db):
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    publisher_id = get_any_publisher(db)
    author_id = get_any_author(db)
    g1 = str(random.randint(1000000, 9999999))
    g2 = str(random.randint(1000000, 9999999))
    genre_table.put_item(Item={
        'id': g1,
        'genre_name': "G3",
        'popularity': Decimal(5)
    })
    genre_table.put_item(Item={
        'id': g2,
        'genre_name': "G4",
        'popularity': Decimal(8)
    })
    g1_isbns = []
    timestamp = int(time.time() * 1000)
    for i in range(2):
        isbn = f"ISBN-{random_suffix()}-{timestamp}-{i}"
        book_table.put_item(Item={
            'ISBN': isbn,
            'Book_Name': "X",
            'Year_Of_Release': Decimal(2021),
            'Genre_Id': g1,
            'Publisher_Id': publisher_id,
            'Author_id': author_id
        })
        g1_isbns.append(isbn)
    g2_isbn = f"ISBN-{random_suffix()}-{timestamp}-2"
    book_table.put_item(Item={
        'ISBN': g2_isbn,
        'Book_Name': "Y",
        'Year_Of_Release': Decimal(2020),
        'Genre_Id': g2,
        'Publisher_Id': publisher_id,
        'Author_id': author_id
    })
    time.sleep(0.2)
    g1_books = []
    for isbn in g1_isbns:
        try:
            response = book_table.get_item(Key={'ISBN': isbn})
            if 'Item' in response and response['Item']['Genre_Id'] == g1:
                g1_books.append(response['Item'])
        except Exception as e:
            pass
    g2_books = []
    try:
        response = book_table.get_item(Key={'ISBN': g2_isbn})
        if 'Item' in response and response['Item']['Genre_Id'] == g2:
            g2_books.append(response['Item'])
    except Exception as e:
        pass
    assert len(g1_books) == 2, f"Oczekiwano 2 książki w gatunku g1, znaleziono {len(g1_books)}"
    assert len(g2_books) == 1, f"Oczekiwano 1 książkę w gatunku g2, znaleziono {len(g2_books)}"

def test_get_users_and_orders_join(db):
    user_table = db.Table('Users')
    order_table = db.Table('Orders')
    book_table = db.Table('Books')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    user_id = str(random.randint(1000000, 9999999))
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    order_id = str(random.randint(1000000, 9999999))
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "JoinLoc",
        'age': Decimal(35)
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "JP",
        'address': "A",
        'country': "PL",
        'email': "jp@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "JA",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "JG",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "JoinBook",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    order_table.put_item(Item={
        'Order_ID': order_id,
        'ISBN': isbn,
        'User_ID': user_id,
        'Order_Date': datetime.now().strftime("%Y-%m-%d"),
        'Order_Cost': Decimal('88.8')
    })
    time.sleep(0.1)
    user = user_table.get_item(Key={'users_id': user_id})['Item']
    order = order_table.get_item(Key={'Order_ID': order_id})['Item']
    assert user['location'] == "JoinLoc"
    assert order['Order_Cost'] == Decimal('88.8')

def get_any_publisher(db):
    table = db.Table('Publishers')
    response = table.scan(Limit=1)
    if response['Items']:
        return response['Items'][0]['publisher_id']
    else:
        pub_id = random_code()
        table.put_item(Item={
            'publisher_id': pub_id,
            'name': "DefaultPub",
            'address': "DefaultAddr",
            'country': "PL",
            'email': "default@mail.com",
            'phone': "000000"
        })
        return pub_id

def get_any_author(db):
    table = db.Table('Authors')
    response = table.scan(Limit=1)
    if response['Items']:
        return response['Items'][0]['author_id']
    else:
        auth_id = random_code()
        table.put_item(Item={
            'author_id': auth_id,
            'author_name': "DefaultAuthor",
            'country_of_origin': "US",
            'birth_date': "1970-01-01"
        })
        return auth_id

def test_update_genre_popularity(db):
    table = db.Table('Book_Genres')
    genre_id = str(random.randint(1000000, 9999999))
    table.put_item(Item={
        'id': genre_id,
        'genre_name': "UG",
        'popularity': Decimal(1)
    })
    table.update_item(
        Key={'id': genre_id},
        UpdateExpression="SET popularity = popularity + :inc",
        ExpressionAttributeValues={':inc': Decimal(5)}
    )
    doc = table.get_item(Key={'id': genre_id})
    assert doc['Item']['popularity'] == Decimal(6)

def test_update_user_location(db):
    table = db.Table('Users')
    user_id = str(random.randint(1000000, 9999999))
    table.put_item(Item={
        'users_id': user_id,
        'location': "OldLoc",
        'age': Decimal(30)
    })
    new_loc = f"Loc-{random_suffix()}"
    table.update_item(
        Key={'users_id': user_id},
        UpdateExpression="SET #loc = :loc",
        ExpressionAttributeNames={'#loc': 'location'},
        ExpressionAttributeValues={':loc': new_loc}
    )
    doc = table.get_item(Key={'users_id': user_id})
    assert doc['Item']['location'] == new_loc

def test_update_genre_popularity_group_by(db):
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    genre_id = str(random.randint(1000000, 9999999))
    publisher_id = get_any_publisher(db)
    author_id = get_any_author(db)
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "UGG",
        'popularity': Decimal(1)
    })
    book_isbns = []
    timestamp = int(time.time() * 1000)
    for i in range(2):
        isbn = f"ISBN-{random_suffix()}-{timestamp}-{i}"
        book_table.put_item(Item={
            'ISBN': isbn,
            'Book_Name': "UGG-Book",
            'Year_Of_Release': Decimal(2021),
            'Genre_Id': genre_id,
            'Publisher_Id': publisher_id,
            'Author_id': author_id
        })
        book_isbns.append(isbn)
    time.sleep(0.1)
    books_count = 0
    for isbn in book_isbns:
        try:
            response = book_table.get_item(Key={'ISBN': isbn})
            if 'Item' in response and response['Item']['Genre_Id'] == genre_id:
                books_count += 1
        except Exception as e:
            pass
    if books_count > 1:
        genre_table.update_item(
            Key={'id': genre_id},
            UpdateExpression="SET popularity = popularity + :inc",
            ExpressionAttributeValues={':inc': Decimal(10)}
        )
    doc = genre_table.get_item(Key={'id': genre_id})
    assert doc['Item']['popularity'] == Decimal(11), f"Oczekiwano popularności 11, znaleziono {doc['Item']['popularity']}, liczba książek: {books_count}"

def test_update_user_with_order_join(db):
    user_table = db.Table('Users')
    order_table = db.Table('Orders')
    book_table = db.Table('Books')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    user_id = str(random.randint(1000000, 9999999))
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    order_id = str(random.randint(1000000, 9999999))
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "OldJoinLoc",
        'age': Decimal(28)
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "PUJ",
        'address': "A",
        'country': "PL",
        'email': "puj@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "AUJ",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "GJ",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "JoinBook2",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    order_table.put_item(Item={
        'Order_ID': order_id,
        'ISBN': isbn,
        'User_ID': user_id,
        'Order_Date': datetime.now().strftime("%Y-%m-%d"),
        'Order_Cost': Decimal('77.7')
    })
    order = order_table.get_item(Key={'Order_ID': order_id})['Item']
    if order['User_ID'] == user_id:
        new_loc = f"JoinLoc-{random_suffix()}"
        user_table.update_item(
            Key={'users_id': user_id},
            UpdateExpression="SET #loc = :loc",
            ExpressionAttributeNames={'#loc': 'location'},
            ExpressionAttributeValues={':loc': new_loc}
        )
    doc = user_table.get_item(Key={'users_id': user_id})
    assert doc['Item']['location'].startswith("JoinLoc-")

def test_delete_genre_by_id(db):
    table = db.Table('Book_Genres')
    genre_id = str(random.randint(1000000, 9999999))
    table.put_item(Item={
        'id': genre_id,
        'genre_name': "DG",
        'popularity': Decimal(3)
    })
    table.delete_item(Key={'id': genre_id})
    doc = table.get_item(Key={'id': genre_id})
    assert 'Item' not in doc

def test_delete_user_by_id(db):
    table = db.Table('Users')
    user_id = str(random.randint(1000000, 9999999))
    table.put_item(Item={
        'users_id': user_id,
        'location': "DU",
        'age': Decimal(25)
    })
    table.delete_item(Key={'users_id': user_id})
    doc = table.get_item(Key={'users_id': user_id})
    assert 'Item' not in doc

def test_delete_books_with_few_ratings_group_by(db):
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    book_table = db.Table('Books')
    user_table = db.Table('Users')
    rating_table = db.Table('Book_Ratings')
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    user_id = str(random.randint(1000000, 9999999))
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "DP",
        'address': "A",
        'country': "PL",
        'email': "dp@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "DA",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "DG",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "DelBook",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "DelLoc",
        'age': Decimal(33)
    })
    rating_table.put_item(Item={
        'Rating_ID': str(random.randint(1000000, 9999999)),
        'Book_Rating': Decimal(5),
        'User_ID': user_id,
        'ISBN': isbn
    })
    ratings_count = rating_table.scan(
        FilterExpression='#isbn = :isbn',
        ExpressionAttributeNames={'#isbn': 'ISBN'},
        ExpressionAttributeValues={':isbn': isbn}
    )['Count']
    if ratings_count < 2:
        book_table.delete_item(Key={'ISBN': isbn})
    doc = book_table.get_item(Key={'ISBN': isbn})
    assert 'Item' not in doc

def test_delete_orders_with_user_join(db):
    user_table = db.Table('Users')
    order_table = db.Table('Orders')
    book_table = db.Table('Books')
    pub_table = db.Table('Publishers')
    auth_table = db.Table('Authors')
    genre_table = db.Table('Book_Genres')
    user_id = str(random.randint(1000000, 9999999))
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    genre_id = str(random.randint(1000000, 9999999))
    order_id = str(random.randint(1000000, 9999999))
    user_table.put_item(Item={
        'users_id': user_id,
        'location': "DelJoinLoc",
        'age': Decimal(45)
    })
    pub_table.put_item(Item={
        'publisher_id': pub_id,
        'name': "DPJ",
        'address': "A",
        'country': "PL",
        'email': "dpj@mail",
        'phone': "000000"
    })
    auth_table.put_item(Item={
        'author_id': auth_id,
        'author_name': "DAJ",
        'country_of_origin': "US",
        'birth_date': "1970-01-01"
    })
    genre_table.put_item(Item={
        'id': genre_id,
        'genre_name': "DGJ",
        'popularity': Decimal(1)
    })
    book_table.put_item(Item={
        'ISBN': isbn,
        'Book_Name': "DelJoinBook",
        'Year_Of_Release': Decimal(2024),
        'Genre_Id': genre_id,
        'Publisher_Id': pub_id,
        'Author_id': auth_id
    })
    order_table.put_item(Item={
        'Order_ID': order_id,
        'ISBN': isbn,
        'User_ID': user_id,
        'Order_Date': datetime.now().strftime("%Y-%m-%d"),
        'Order_Cost': Decimal('66.6')
    })
    user = user_table.get_item(Key={'users_id': user_id})['Item']
    if user['location'] == "DelJoinLoc":
        order_table.delete_item(Key={'Order_ID': order_id})
    doc = order_table.get_item(Key={'Order_ID': order_id})
    assert 'Item' not in doc


def dynamo_tests():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='eu-north-1',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    tests = [
        test_insert_book_genre,
        test_insert_user,
        test_insert_publisher_and_author,
        test_insert_order_and_return,
        test_insert_book_rating_group_by,
        test_insert_book_rating_join,
        test_get_order_with_book_and_author,
        test_get_average_book_rating_above,
        test_get_genre_book_counts_group_by,
        test_get_users_and_orders_join,
        test_update_genre_popularity,
        test_update_user_location,
        test_update_genre_popularity_group_by,
        test_update_user_with_order_join,
        test_delete_genre_by_id,
        test_delete_user_by_id,
        test_delete_books_with_few_ratings_group_by,
        test_delete_orders_with_user_join,
    ]

    crud_categories = {
        'CREATE': [
            'test_insert_book_genre',
            'test_insert_user',
            'test_insert_publisher_and_author',
            'test_insert_order_and_return',
            'test_insert_book_rating_group_by',
            'test_insert_book_rating_join'
        ],
        'READ': [
            'test_get_order_with_book_and_author',
            'test_get_average_book_rating_above',
            'test_get_genre_book_counts_group_by',
            'test_get_users_and_orders_join'
        ],
        'UPDATE': [
            'test_update_genre_popularity',
            'test_update_user_location',
            'test_update_genre_popularity_group_by',
            'test_update_user_with_order_join'
        ],
        'DELETE': [
            'test_delete_genre_by_id',
            'test_delete_user_by_id',
            'test_delete_books_with_few_ratings_group_by',
            'test_delete_orders_with_user_join'
        ]
    }

    data_sizes = [50, 100]
    runs_per_test = 2

    print(f"Znaleziono {len(tests)} testów DynamoDB")

    total_start = time.time()
    individual_results = {}
    crud_results = {}

    for data_size in data_sizes:
        print(f"\n{'=' * 60}")
        print(f"TESTY DYNAMODB DLA ROZMIARU DANYCH: {data_size}")
        print(f"{'=' * 60}")

        individual_results[data_size] = {}
        crud_results[data_size] = {
            'CREATE': [],
            'READ': [],
            'UPDATE': [],
            'DELETE': []
        }

        prepare_dynamo_test_data(dynamodb, data_size)

        for test in tests:
            times = []
            status = "OK"
            for run in range(runs_per_test):
                start = time.time()
                try:
                    test(dynamodb)
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
                min_time = min(times)
                max_time = max(times)
                print(f"{test.__name__:35} → {status:10} {avg_time:.4f}s (śr.) [{min_time:.4f}s - {max_time:.4f}s]")

                test_name = test.__name__

                # Zapisz wynik dla pojedynczego testu
                individual_results[data_size][test_name] = avg_time

                # Dodaj do kategorii CRUD
                for crud_op, test_names in crud_categories.items():
                    if test_name in test_names:
                        crud_results[data_size][crud_op].append(avg_time)
                        break
            else:
                print(f"{test.__name__:35} → {status:10} (test nie przeszedł)")

        cleanup_dynamo_test_data(dynamodb)

    # Zwróć oba typy wyników
    final_results = {}
    for data_size in data_sizes:
        final_results[data_size] = {}
        # CRUD sums
        for crud_op in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
            times = crud_results[data_size][crud_op]
            if times:
                final_results[data_size][crud_op] = sum(times)
            else:
                final_results[data_size][crud_op] = 0.0

        # Individual test results
        for test_name, time_val in individual_results[data_size].items():
            final_results[data_size][test_name] = time_val

    return final_results


def prepare_dynamo_test_data(db, size):
    batch_size = 1000
    genres_data = []
    for i in range(20):
        genres_data.append({
            'id': str(1000000 + i),
            'genre_name': f"TestGenre_{i}",
            'popularity': Decimal(i % 10 + 1)
        })
    publishers_data = []
    for i in range(50):
        publishers_data.append({
            'publisher_id': f"TESTPUB{i:04d}",
            'name': f"TestPublisher_{i}",
            'address': f"TestAddress_{i}",
            'country': f"TestCountry_{i % 10}",
            'email': f"publisher{i}@test.com",
            'phone': f"123-456-{i:04d}"
        })
    authors_data = []
    for i in range(100):
        authors_data.append({
            'author_id': f"TESTAUTH{i:04d}",
            'author_name': f"TestAuthor_{i}",
            'country_of_origin': f"TestCountry_{i % 20}",
            'birth_date': f"19{50 + i % 50}-01-01"
        })
    users_data = []
    for i in range(size):
        users_data.append({
            'users_id': str(2000000 + i),
            'location': f"TestCity_{i % 100}",
            'age': Decimal(18 + (i % 62))
        })
    genre_ids = [g['id'] for g in genres_data]
    publisher_ids = [p['publisher_id'] for p in publishers_data]
    author_ids = [a['author_id'] for a in authors_data]
    books_data = []
    for i in range(size):
        books_data.append({
            'ISBN': f"TEST-{i:010d}",
            'Book_Name': f"TestBook_{i}",
            'Year_Of_Release': Decimal(1950 + (i % 74)),
            'Genre_Id': genre_ids[i % len(genre_ids)],
            'Publisher_Id': publisher_ids[i % len(publisher_ids)],
            'Author_id': author_ids[i % len(author_ids)]
        })
    user_ids = [u['users_id'] for u in users_data]
    isbns = [b['ISBN'] for b in books_data]
    ratings_data = []
    rating_limit = min(size, len(user_ids) * len(isbns) // 10)
    for i in range(rating_limit):
        ratings_data.append({
            'Rating_ID': str(4000000 + i),
            'Book_Rating': Decimal((i % 5) + 1),
            'User_ID': user_ids[i % len(user_ids)],
            'ISBN': isbns[i % len(isbns)]
        })
    orders_data = []
    for i in range(size // 2):
        orders_data.append({
            'Order_ID': str(3000000 + i),
            'ISBN': isbns[i % len(isbns)],
            'User_ID': user_ids[i % len(user_ids)],
            'Order_Date': f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            'Order_Cost': Decimal(str(round(10.0 + (i % 50), 2)))
        })
    returns_data = []
    for i in range(size // 10):
        returns_data.append({
            'Return_ID': str(5000000 + i),
            'Order_ID': str(3000000 + (i % len(orders_data))),
            'Return_Date': f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            'Reason_Description': f"TestReason_{i % 5}"
        })
    def batch_write(table_name, items):
        table = db.Table(table_name)
        for i in range(0, len(items), batch_size):
            with table.batch_writer() as batch:
                for item in items[i:i+batch_size]:
                    batch.put_item(Item=item)
    print(f"Przygotowywanie {size} rekordów testowych DynamoDB...")
    batch_write('Book_Genres', genres_data)
    batch_write('Publishers', publishers_data)
    batch_write('Authors', authors_data)
    batch_write('Users', users_data)
    batch_write('Books', books_data)
    if ratings_data:
        batch_write('Book_Ratings', ratings_data)
    if orders_data:
        batch_write('Orders', orders_data)
    if returns_data:
        batch_write('Returns', returns_data)
    print(f"Dane testowe DynamoDB przygotowane.")

def cleanup_dynamo_test_data(db):
    def delete_by_keys(table_name, key_name, key_values):
        table = db.Table(table_name)
        for i in range(0, len(key_values), 1000):
            with table.batch_writer() as batch:
                for key in key_values[i:i+1000]:
                    batch.delete_item(Key={key_name: key})
    genres_table = db.Table('Book_Genres')
    genres = genres_table.scan(
        FilterExpression="begins_with(genre_name, :prefix)",
        ExpressionAttributeValues={":prefix": "TestGenre_"}
    )['Items']
    genre_ids = [g['id'] for g in genres]
    publishers_table = db.Table('Publishers')
    publishers = publishers_table.scan(
        FilterExpression="begins_with(#nazwa, :prefix)",
        ExpressionAttributeNames={"#nazwa": "name"},
        ExpressionAttributeValues={":prefix": "TestPublisher_"}
    )['Items']
    publisher_ids = [p['publisher_id'] for p in publishers]
    authors_table = db.Table('Authors')
    authors = authors_table.scan(
        FilterExpression="begins_with(author_name, :prefix)",
        ExpressionAttributeValues={":prefix": "TestAuthor_"}
    )['Items']
    author_ids = [a['author_id'] for a in authors]
    users_table = db.Table('Users')
    users = users_table.scan(
        FilterExpression="begins_with(#loc, :prefix)",
        ExpressionAttributeNames={"#loc": "location"},
        ExpressionAttributeValues={":prefix": "TestCity_"}
    )['Items']
    user_ids = [u['users_id'] for u in users]
    books_table = db.Table('Books')
    books = books_table.scan(
        FilterExpression="begins_with(Book_Name, :prefix)",
        ExpressionAttributeValues={":prefix": "TestBook_"}
    )['Items']
    isbns = [b['ISBN'] for b in books]
    ratings_table = db.Table('Book_Ratings')
    ratings = ratings_table.scan(
        FilterExpression="Rating_ID >= :minid",
        ExpressionAttributeValues={":minid": "4000000"}
    )['Items']
    rating_ids = [r['Rating_ID'] for r in ratings]
    orders_table = db.Table('Orders')
    orders = orders_table.scan(
        FilterExpression="Order_ID >= :minid",
        ExpressionAttributeValues={":minid": "3000000"}
    )['Items']
    order_ids = [o['Order_ID'] for o in orders]
    returns_table = db.Table('Returns')
    returns = returns_table.scan(
        FilterExpression="Return_ID >= :minid",
        ExpressionAttributeValues={":minid": "5000000"}
    )['Items']
    return_ids = [r['Return_ID'] for r in returns]
    print("Czyszczenie danych testowych DynamoDB...")
    if genre_ids:
        delete_by_keys('Book_Genres', 'id', genre_ids)
    if publisher_ids:
        delete_by_keys('Publishers', 'publisher_id', publisher_ids)
    if author_ids:
        delete_by_keys('Authors', 'author_id', author_ids)
    if user_ids:
        delete_by_keys('Users', 'users_id', user_ids)
    if isbns:
        delete_by_keys('Books', 'ISBN', isbns)
    if rating_ids:
        delete_by_keys('Book_Ratings', 'Rating_ID', rating_ids)
    if order_ids:
        delete_by_keys('Orders', 'Order_ID', order_ids)
    if return_ids:
        delete_by_keys('Returns', 'Return_ID', return_ids)
    print("Dane testowe DynamoDB zostały wyczyszczone.")
