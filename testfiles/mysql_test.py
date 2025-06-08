import pytest
import mysql.connector
import random
import string
import time
import inspect, sys


@pytest.fixture(scope="module")
def conn():
    c = mysql.connector.connect(
        host="localhost",
        user="root",
        password="my-secret-pw",
        database="ZTB_DATABASE",
        auth_plugin='mysql_native_password'
    )
    yield c
    c.close()


def random_suffix(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def random_isbn():
    return ''.join(random.choices(string.digits, k=10))


def random_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))


# -----------------------
# POST tests
# -----------------------

def test_insert_book_genre(conn):
    cur = conn.cursor()
    genre_name = f"Genre-{random_suffix()}"
    pop = random.randint(1, 100)
    cur.execute(
        "INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)",
        (genre_name, pop)
    )
    conn.commit()
    gid = cur.lastrowid
    assert gid > 0
    cur.execute("SELECT genre_name,popularity FROM Book_Genres WHERE id=%s", (gid,))
    assert cur.fetchone() == (genre_name, pop)


def test_insert_user(conn):
    cur = conn.cursor()
    loc = f"City-{random_suffix()}"
    age = random.randint(18, 80)
    cur.execute(
        "INSERT INTO Users (location,age) VALUES (%s,%s)",
        (loc, age)
    )
    conn.commit()
    uid = cur.lastrowid
    assert uid > 0
    cur.execute("SELECT location,age FROM Users WHERE users_id=%s", (uid,))
    assert cur.fetchone() == (loc, age)


def test_insert_publisher_and_author(conn):
    cur = conn.cursor()
    pub_id = random_code()
    auth_id = random_code()
    name = f"Pub-{random_suffix()}"
    cur.execute(
        "INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
        (pub_id, name, "Addr", "PL", f"{name}@mail.com", "123456")
    )
    cur.execute(
        "INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
        (auth_id, f"Auth-{random_suffix()}", "US", "1970-01-01")
    )
    conn.commit()
    cur.execute("SELECT name FROM Publishers WHERE publisher_id=%s", (pub_id,))
    assert cur.fetchone() is not None
    cur.execute("SELECT author_name FROM Authors WHERE author_id=%s", (auth_id,))
    assert cur.fetchone() is not None


def test_insert_order_and_return(conn):
    cur = conn.cursor()
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "P", "A", "PL", "p@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "A", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "T", 2025, gid, pub_id, auth_id)
    )
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("L", 30))
    uid = cur.lastrowid
    cur.execute(
        "INSERT INTO Orders (isbn,user_id,order_date,order_cost) VALUES (%s,%s,CURDATE(),%s)",
        (isbn, uid, 99.9)
    )
    oid = cur.lastrowid
    cur.execute(
        "INSERT INTO Returns (order_id,return_date,reason_description) VALUES (%s,CURDATE(),%s)",
        (oid, "no reason")
    )
    rid = cur.lastrowid
    conn.commit()
    assert oid > 0 and rid > 0


def test_insert_book_rating_group_by(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("X", 40))
    uid = cur.lastrowid
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "P", "A", "PL", "p@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "A", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "BR", 2024, gid, pub_id, auth_id)
    )
    for rating in [3, 4, 5]:
        cur.execute("INSERT INTO Book_Ratings (user_id,isbn,book_rating) VALUES (%s,%s,%s)", (uid, isbn, rating))
    conn.commit()
    cur.execute("SELECT isbn, COUNT(*) FROM Book_Ratings GROUP BY isbn HAVING isbn=%s", (isbn,))
    row = cur.fetchone()
    assert row[1] == 3


def test_insert_book_rating_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("Y", 50))
    uid = cur.lastrowid
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "P", "A", "PL", "p@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "A", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "BRJ", 2024, gid, pub_id, auth_id)
    )
    rating = random.randint(1, 5)
    cur.execute("INSERT INTO Book_Ratings (user_id,isbn,book_rating) VALUES (%s,%s,%s)", (uid, isbn, rating))
    conn.commit()
    cur.execute("""
        SELECT br.book_rating, b.book_name
        FROM Book_Ratings br
        JOIN Books b ON br.isbn = b.isbn
        WHERE br.user_id=%s AND br.isbn=%s
    """, (uid, isbn))
    row = cur.fetchone()
    assert row[0] == rating and row[1] == "BRJ"


# -----------------------
# GET tests
# -----------------------

def test_get_order_with_book_and_author(conn):
    cur = conn.cursor()
    pub_id = random_code()
    auth_id = random_code()
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "GA", "FR", "1985-05-05"))
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "PB", "A", "DE", "pb@mail", "000000"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G2", 15))
    gid = cur.lastrowid
    isbn = random_isbn()
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "Name", 2023, gid, pub_id, auth_id)
    )
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("Loc", 22))
    uid = cur.lastrowid
    cur.execute("INSERT INTO Orders (isbn,user_id,order_date,order_cost) VALUES (%s,%s,CURDATE(),%s)",
                (isbn, uid, 55.5))
    oid = cur.lastrowid
    conn.commit()
    cur.execute("""
        SELECT o.order_id, b.book_name, a.author_name
        FROM Orders o
        JOIN Books b ON o.isbn=b.isbn
        JOIN Authors a ON b.author_id=a.author_id
        WHERE o.order_id=%s
    """, (oid,))
    row = cur.fetchone()
    assert row == (oid, "Name", "GA")


def test_get_average_book_rating_above(conn):
    cur = conn.cursor()
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "P", "A", "PL", "p@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "A", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "Avg", 2022, gid, pub_id, auth_id))
    vals = []
    for r in [3, 5, 4]:
        cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("U", 30))
        uid = cur.lastrowid
        cur.execute("INSERT INTO Book_Ratings (user_id,isbn,book_rating) VALUES (%s,%s,%s)", (uid, isbn, r))
        vals.append(r)
    conn.commit()
    avg = sum(vals) / len(vals)
    cur.execute("""
        SELECT isbn FROM Books
        WHERE isbn IN (
            SELECT isbn FROM Book_Ratings GROUP BY isbn HAVING AVG(book_rating) > %s
        )
    """, (3.5,))
    fetched = [r[0] for r in cur.fetchall()]
    assert isbn in fetched


def test_get_genre_book_counts_group_by(conn):
    cur = conn.cursor()
    publisher_id = get_any(cur, "Publishers", "publisher_id")
    author_id = get_any(cur, "Authors", "author_id")
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G3", 5))
    g1 = cur.lastrowid
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("G4", 8))
    g2 = cur.lastrowid
    for _ in range(2):
        cur.execute("INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (f"ISBN-{random_suffix()}", "X", 2021, g1, publisher_id, author_id))
    cur.execute("INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (f"ISBN-{random_suffix()}", "Y", 2020, g2, publisher_id, author_id))
    conn.commit()
    cur.execute("""
        SELECT g.genre_name, COUNT(b.isbn) AS cnt
        FROM Book_Genres g
        LEFT JOIN Books b ON g.id=b.genre_id
        WHERE g.id IN (%s,%s)
        GROUP BY g.genre_name
    """, (g1, g2))
    res = dict(cur.fetchall())
    assert res["G3"] == 2 and res["G4"] == 1


def test_get_users_and_orders_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("JoinLoc", 35))
    uid = cur.lastrowid
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "JP", "A", "PL", "jp@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "JA", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("JG", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "JoinBook", 2024, gid, pub_id, auth_id)
    )
    cur.execute("INSERT INTO Orders (isbn,user_id,order_date,order_cost) VALUES (%s,%s,CURDATE(),%s)",
                (isbn, uid, 88.8))
    conn.commit()
    cur.execute("""
        SELECT u.location, o.order_cost
        FROM Users u
        JOIN Orders o ON u.users_id = o.user_id
        WHERE u.users_id = %s
    """, (uid,))
    row = cur.fetchone()
    assert row[0] == "JoinLoc" and float(row[1]) == 88.8


def get_any(cur, table, col):
    cur.execute(f"SELECT {col} FROM {table} LIMIT 1")
    row = cur.fetchone()
    if row:
        return row[0]
    raise Exception(f"No data in {table}")


# -----------------------
# PUT tests
# -----------------------

def test_update_genre_popularity(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("UG", 1))
    gid = cur.lastrowid
    conn.commit()
    cur.execute("UPDATE Book_Genres SET popularity=popularity+5 WHERE id=%s", (gid,))
    conn.commit()
    cur.execute("SELECT popularity FROM Book_Genres WHERE id=%s", (gid,))
    assert cur.fetchone()[0] == 6


def test_update_user_location(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("OldLoc", 30))
    uid = cur.lastrowid
    conn.commit()
    new_loc = f"Loc-{random_suffix()}"
    cur.execute("UPDATE Users SET location=%s WHERE users_id=%s", (new_loc, uid))
    conn.commit()
    cur.execute("SELECT location FROM Users WHERE users_id=%s", (uid,))
    assert cur.fetchone()[0] == new_loc


def test_update_genre_popularity_group_by(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("UGG", 1))
    gid = cur.lastrowid
    publisher_id = get_any(cur, "Publishers", "publisher_id")
    author_id = get_any(cur, "Authors", "author_id")
    for _ in range(2):
        cur.execute("INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (f"ISBN-{random_suffix()}", "UGG-Book", 2021, gid, publisher_id, author_id))
    conn.commit()
    cur.execute("""
        UPDATE Book_Genres
        SET popularity = popularity + 10
        WHERE id IN (
            SELECT genre_id FROM Books GROUP BY genre_id HAVING COUNT(*) > 1
        )
    """)
    conn.commit()
    cur.execute("SELECT popularity FROM Book_Genres WHERE id=%s", (gid,))
    assert cur.fetchone()[0] == 11


def test_update_user_with_order_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("OldJoinLoc", 28))
    uid = cur.lastrowid
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "PUJ", "A", "PL", "puj@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "AUJ", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("GJ", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "JoinBook2", 2024, gid, pub_id, auth_id)
    )
    cur.execute("INSERT INTO Orders (isbn,user_id,order_date,order_cost) VALUES (%s,%s,CURDATE(),%s)",
                (isbn, uid, 77.7))
    conn.commit()
    new_loc = f"JoinLoc-{random_suffix()}"
    cur.execute("""
        UPDATE Users u
        JOIN Orders o ON u.users_id = o.user_id
        SET u.location = %s
        WHERE o.user_id = %s
    """, (new_loc, uid))
    conn.commit()
    cur.execute("SELECT location FROM Users WHERE users_id=%s", (uid,))
    assert cur.fetchone()[0] == new_loc


# -----------------------
# DELETE tests
# -----------------------

def test_delete_genre_by_id(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("DG", 3))
    gid = cur.lastrowid
    conn.commit()
    cur.execute("DELETE FROM Book_Genres WHERE id=%s", (gid,))
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM Book_Genres WHERE id=%s", (gid,))
    assert cur.fetchone()[0] == 0


def test_delete_user_by_id(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("DU", 25))
    uid = cur.lastrowid
    conn.commit()
    cur.execute("DELETE FROM Users WHERE users_id=%s", (uid,))
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM Users WHERE users_id=%s", (uid,))
    assert cur.fetchone()[0] == 0


def test_delete_books_with_few_ratings_group_by(conn):
    cur = conn.cursor()
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "DP", "A", "PL", "dp@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "DA", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("DG", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "DelBook", 2024, gid, pub_id, auth_id))
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("DelLoc", 33))
    uid = cur.lastrowid
    cur.execute("INSERT INTO Book_Ratings (user_id,isbn,book_rating) VALUES (%s,%s,%s)", (uid, isbn, 5))
    conn.commit()
    cur.execute("""
        DELETE FROM Books
        WHERE isbn IN (
            SELECT isbn FROM Book_Ratings GROUP BY isbn HAVING COUNT(*) < 2
        )
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM Books WHERE isbn=%s", (isbn,))
    assert cur.fetchone()[0] == 0


def test_delete_orders_with_user_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (location,age) VALUES (%s,%s)", ("DelJoinLoc", 45))
    uid = cur.lastrowid
    pub_id = random_code()
    auth_id = random_code()
    isbn = random_isbn()
    cur.execute("INSERT INTO Publishers (publisher_id, name,address,country,email,phone) VALUES (%s,%s,%s,%s,%s,%s)",
                (pub_id, "DPJ", "A", "PL", "dpj@mail", "000000"))
    cur.execute("INSERT INTO Authors (author_id, author_name,country_of_origin,birth_date) VALUES (%s,%s,%s,%s)",
                (auth_id, "DAJ", "US", "1970-01-01"))
    cur.execute("INSERT INTO Book_Genres (genre_name,popularity) VALUES (%s,%s)", ("DGJ", 1))
    gid = cur.lastrowid
    cur.execute(
        "INSERT INTO Books (isbn,book_name,year_of_release,genre_id,publisher_id,author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "DelJoinBook", 2024, gid, pub_id, auth_id))
    cur.execute("INSERT INTO Orders (isbn,user_id,order_date,order_cost) VALUES (%s,%s,CURDATE(),%s)",
                (isbn, uid, 66.6))
    conn.commit()
    cur.execute("""
        DELETE o FROM Orders o
        JOIN Users u ON o.user_id = u.users_id
        WHERE u.location = %s
    """, ("DelJoinLoc",))
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM Orders WHERE user_id=%s", (uid,))
    assert cur.fetchone()[0] == 0


# -----------------------
# TEST START
# -----------------------

def mysql_tests():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="my-secret-pw",
        database="ZTB_DATABASE",
        auth_plugin='mysql_native_password'
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

    data_sizes = [500000]
    runs_per_test = 5

    print(f"Znaleziono {len(tests)} testów MySQL")
    print(f"Każdy test będzie uruchomiony {runs_per_test} razy dla każdego z {len(data_sizes)} rozmiarów danych")

    total_start = time.time()
    individual_results = {}
    crud_results = {}

    for data_size in data_sizes:
        individual_results[data_size] = {}
        crud_results[data_size] = {
            'CREATE': [],
            'READ': [],
            'UPDATE': [],
            'DELETE': []
        }

        prepare_test_data(conn, data_size)

        for test in tests:
            times = []
            status = "OK"
            for run in range(runs_per_test):
                start = time.time()
                try:
                    test(conn)
                    duration = time.time() - start
                    times.append(duration)
                except Exception as e:
                    status = f"ERROR ({e.__class__.__name__})"
                    break

            if times:
                avg_time = sum(times) / len(times)
                test_name = test.__name__

                # Zapisz wynik dla pojedynczego testu
                individual_results[data_size][test_name] = avg_time

                # Dodaj do kategorii CRUD
                for crud_op, test_names in crud_categories.items():
                    if test_name in test_names:
                        crud_results[data_size][crud_op].append(avg_time)
                        break

        cleanup_test_data(conn)

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

    conn.close()
    return final_results


def prepare_test_data(conn, size):
    cursor = conn.cursor()
    try:
        cleanup_test_data(conn)
        print(f"Przygotowywanie {size} rekordów danych testowych...")

        genres_data = []
        for i in range(20):
            genre_name = f"TestGenre_{i}"
            popularity = i % 10 + 1
            genres_data.append((genre_name, popularity))
        cursor.executemany(
            "INSERT IGNORE INTO Book_Genres (genre_name, popularity) VALUES (%s, %s)",
            genres_data
        )

        publishers_data = []
        for i in range(50):
            name = f"TestPublisher_{i}"
            address = f"TestAddress_{i}"
            country = f"TestCountry_{i % 10}"
            email = f"publisher{i}@test.com"
            phone = f"123-456-{i:04d}"
            publishers_data.append((name, address, country, email, phone))
        cursor.executemany(
            "INSERT IGNORE INTO Publishers (name, address, country, email, phone) VALUES (%s, %s, %s, %s, %s)",
            publishers_data
        )

        authors_data = []
        for i in range(100):
            author_name = f"TestAuthor_{i}"
            country_of_origin = f"TestCountry_{i % 20}"
            birth_date = f"19{50 + i % 50}-01-01"
            authors_data.append((author_name, country_of_origin, birth_date))
        cursor.executemany(
            "INSERT IGNORE INTO Authors (author_name, country_of_origin, birth_date) VALUES (%s, %s, %s)",
            authors_data
        )

        cursor.execute("SELECT id FROM Book_Genres WHERE genre_name LIKE 'TestGenre_%'")
        genre_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT publisher_id FROM Publishers WHERE name LIKE 'TestPublisher_%'")
        publisher_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT author_id FROM Authors WHERE author_name LIKE 'TestAuthor_%'")
        author_ids = [row[0] for row in cursor.fetchall()]

        users_data = []
        for i in range(size):
            location = f"TestCity_{i % 100}"
            age = 18 + (i % 62)
            users_data.append((location, age))
        cursor.executemany(
            "INSERT INTO Users (location, age) VALUES (%s, %s)",
            users_data
        )

        books_data = []
        cursor.execute("SELECT MAX(CAST(SUBSTRING(ISBN, 6) AS UNSIGNED)) FROM Books WHERE ISBN LIKE 'TEST-%'")
        result = cursor.fetchone()
        max_existing = result[0] if result[0] is not None else -1
        for i in range(size):
            isbn = f"TEST-{max_existing + i + 1:010d}"
            book_name = f"TestBook_{max_existing + i + 1}"
            year_of_release = 1950 + (i % 74)
            genre_id = genre_ids[i % len(genre_ids)]
            publisher_id = publisher_ids[i % len(publisher_ids)]
            author_id = author_ids[i % len(author_ids)]
            books_data.append((isbn, book_name, year_of_release, genre_id, publisher_id, author_id))
        cursor.executemany(
            "INSERT INTO Books (ISBN, Book_Name, Year_Of_Release, Genre_Id, Publisher_id, Author_id) VALUES (%s, %s, %s, %s, %s, %s)",
            books_data
        )

        cursor.execute("SELECT users_id FROM Users WHERE location LIKE 'TestCity_%'")
        user_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT ISBN FROM Books WHERE Book_Name LIKE 'TestBook_%'")
        book_isbns = [row[0] for row in cursor.fetchall()]

        ratings_data = []
        for i in range(min(size, len(user_ids) * len(book_isbns) // 10)):
            user_id = user_ids[i % len(user_ids)]
            isbn = book_isbns[i % len(book_isbns)]
            rating = (i % 5) + 1
            ratings_data.append((user_id, isbn, rating))
        if ratings_data:
            cursor.executemany(
                "INSERT IGNORE INTO Book_Ratings (User_ID, ISBN, Book_Rating) VALUES (%s, %s, %s)",
                ratings_data
            )

        orders_data = []
        for i in range(size // 2):
            isbn = book_isbns[i % len(book_isbns)]
            user_id = user_ids[i % len(user_ids)]
            order_date = f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
            order_cost = round(10.0 + (i % 50), 2)
            orders_data.append((isbn, user_id, order_date, order_cost))
        if orders_data:
            cursor.executemany(
                "INSERT INTO Orders (ISBN, User_ID, Order_Date, Order_Cost) VALUES (%s, %s, %s, %s)",
                orders_data
            )

        conn.commit()
        print(f"Przygotowano dane testowe o rozmiarze {size}")

    except Exception as e:
        print(f"Błąd podczas przygotowywania danych: {e}")
        conn.rollback()
    finally:
        cursor.close()


def cleanup_test_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute(
            "DELETE FROM Orders WHERE User_ID IN (SELECT users_id FROM Users WHERE location LIKE 'TestCity_%')")
        cursor.execute(
            "DELETE FROM Book_Ratings WHERE ISBN IN (SELECT ISBN FROM Books WHERE Book_Name LIKE 'TestBook_%')")
        cursor.execute("DELETE FROM Books WHERE Book_Name LIKE 'TestBook_%'")
        cursor.execute("DELETE FROM Users WHERE location LIKE 'TestCity_%'")
        cursor.execute("DELETE FROM Authors WHERE author_name LIKE 'TestAuthor_%'")
        cursor.execute("DELETE FROM Publishers WHERE name LIKE 'TestPublisher_%'")
        cursor.execute("DELETE FROM Book_Genres WHERE genre_name LIKE 'TestGenre_%'")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
    except Exception as e:
        print(f"Błąd podczas czyszczenia danych: {e}")
        conn.rollback()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
        except:
            pass
    finally:
        cursor.close()
