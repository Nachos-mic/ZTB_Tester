import pytest
import psycopg2
import random
import string
import time
import inspect, sys


@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="my-secret-password",
        database="postgres",
        port="5432"
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
        "INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)",
        (genre_name, pop)
    )
    conn.commit()

    cur.execute("SELECT genre, popularity FROM book_genres WHERE genre = %s", (genre_name,))
    result = cur.fetchone()
    assert result == (genre_name, pop)


def test_insert_user(conn):
    cur = conn.cursor()
    loc = f"City-{random_suffix()}"
    age = random.randint(18, 80)
    cur.execute(
        "INSERT INTO users (location, age) VALUES (%s,%s)",
        (loc, age)
    )
    conn.commit()

    cur.execute("SELECT location, age FROM users WHERE location = %s", (loc,))
    result = cur.fetchone()
    assert result == (loc, age)


def test_insert_publisher_and_author(conn):
    cur = conn.cursor()
    pub_name = f"Pub-{random_suffix()}"
    auth_name = f"Auth-{random_suffix()}"

    cur.execute(
        "INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
        (pub_name, "Addr", "PL", f"{pub_name}@mail.com", "123456")
    )

    cur.execute(
        "INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
        (auth_name, "US", "1970-01-01")
    )
    conn.commit()

    cur.execute("SELECT name FROM publishers WHERE name = %s", (pub_name,))
    assert cur.fetchone() is not None

    cur.execute("SELECT author_name FROM authors WHERE author_name = %s", (auth_name,))
    assert cur.fetchone() is not None


def test_insert_order_and_return(conn):
    cur = conn.cursor()
    pub_name = f"P-{random_suffix()}"
    auth_name = f"A-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "p@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "T", 2025, genre_id, pub_id, auth_id)
    )

    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("L", 30))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("L",))
    uid = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s,%s,CURRENT_DATE,%s)",
        (isbn, uid, 99.9)
    )
    cur.execute("SELECT order_id FROM orders WHERE isbn = %s AND user_id = %s", (isbn, uid))
    oid = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO returns (order_id, return_date, reason_description) VALUES (%s,CURRENT_DATE,%s)",
        (oid, "no reason")
    )
    cur.execute("SELECT return_id FROM returns WHERE order_id = %s", (oid,))
    rid = cur.fetchone()[0]

    conn.commit()
    assert oid > 0 and rid > 0


def test_insert_book_rating_group_by(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("X", 40))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("X",))
    uid = cur.fetchone()[0]

    pub_name = f"P-{random_suffix()}"
    auth_name = f"A-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "p@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "BR", 2024, genre_id, pub_id, auth_id)
    )

    for rating in [3, 4, 5]:
        cur.execute("INSERT INTO book_ratings (user_id, isbn, book_rating) VALUES (%s,%s,%s)", (uid, isbn, rating))

    conn.commit()

    cur.execute("SELECT isbn, COUNT(*) FROM book_ratings WHERE isbn = %s GROUP BY isbn", (isbn,))
    row = cur.fetchone()
    assert row[1] == 3


def test_insert_book_rating_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("Y", 50))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("Y",))
    uid = cur.fetchone()[0]

    pub_name = f"P-{random_suffix()}"
    auth_name = f"A-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "p@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "BRJ", 2024, genre_id, pub_id, auth_id)
    )

    rating = random.randint(1, 5)
    cur.execute("INSERT INTO book_ratings (user_id, isbn, book_rating) VALUES (%s,%s,%s)", (uid, isbn, rating))

    conn.commit()

    cur.execute("""
        SELECT br.book_rating, b.book_name
        FROM book_ratings br
        JOIN books b ON br.isbn = b.isbn
        WHERE br.user_id = %s AND br.isbn = %s
    """, (uid, isbn))

    row = cur.fetchone()
    assert row[0] == rating and row[1] == "BRJ"


# -----------------------
# GET tests
# -----------------------

def test_get_order_with_book_and_author(conn):
    cur = conn.cursor()
    pub_name = f"PB-{random_suffix()}"
    auth_name = f"GA-{random_suffix()}"

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "FR", "1985-05-05"))

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "DE", "pb@mail", "000000"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G2", 15))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G2",))
    genre_id = cur.fetchone()[0]

    isbn = random_isbn()
    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "Name", 2023, genre_id, pub_id, auth_id)
    )

    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("Loc", 22))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("Loc",))
    uid = cur.fetchone()[0]

    cur.execute("INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s,%s,CURRENT_DATE,%s)",
                (isbn, uid, 55.5))
    cur.execute("SELECT order_id FROM orders WHERE isbn = %s AND user_id = %s", (isbn, uid))
    oid = cur.fetchone()[0]

    conn.commit()

    cur.execute("""
        SELECT o.order_id, b.book_name, a.author_name
        FROM orders o
        JOIN books b ON o.isbn = b.isbn
        JOIN authors a ON b.author_id = a.author_id
        WHERE o.order_id = %s
    """, (oid,))

    row = cur.fetchone()
    assert row == (oid, "Name", auth_name)


def test_get_average_book_rating_above(conn):
    cur = conn.cursor()
    pub_name = f"P-{random_suffix()}"
    auth_name = f"A-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "p@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "Avg", 2022, genre_id, pub_id, auth_id))

    vals = []
    for r in [3, 5, 4]:
        cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", (f"U{r}", 30))
        cur.execute("SELECT users_id FROM users WHERE location = %s", (f"U{r}",))
        uid = cur.fetchone()[0]
        cur.execute("INSERT INTO book_ratings (user_id, isbn, book_rating) VALUES (%s,%s,%s)", (uid, isbn, r))
        vals.append(r)

    conn.commit()

    avg = sum(vals) / len(vals)
    cur.execute("""
        SELECT isbn FROM books
        WHERE isbn IN (
            SELECT isbn FROM book_ratings GROUP BY isbn HAVING AVG(book_rating) > %s
        )
    """, (3.5,))

    fetched = [r[0] for r in cur.fetchall()]
    assert isbn in fetched


def test_get_genre_book_counts_group_by(conn):
    conn.rollback()

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM books WHERE book_name IN ('X', 'Y')")
        cur.execute("DELETE FROM book_genres WHERE genre IN ('G3', 'G4')")

        publisher_id = get_any(cur, "publishers", "publisher_id")
        author_id = get_any(cur, "authors", "author_id")

        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G3", 5))
        cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G3",))
        g1 = cur.fetchone()[0]

        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("G4", 8))
        cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("G4",))
        g2 = cur.fetchone()[0]

        for _ in range(2):
            cur.execute("INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (f"ISBN-{random_suffix()}", "X", 2021, g1, publisher_id, author_id))

        cur.execute("INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (f"ISBN-{random_suffix()}", "Y", 2020, g2, publisher_id, author_id))

        cur.execute("""
            SELECT g.genre, COUNT(b.isbn) AS cnt
            FROM book_genres g
            LEFT JOIN books b ON g.id = b.genre_id
            WHERE g.id IN (%s,%s)
            GROUP BY g.genre
        """, (g1, g2))

        res = dict(cur.fetchall())
        assert res["G3"] == 2 and res["G4"] == 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise
    finally:
        try:
            cur.execute("DELETE FROM books WHERE book_name IN ('X', 'Y')")
            cur.execute("DELETE FROM book_genres WHERE genre IN ('G3', 'G4')")
            conn.commit()
        except Exception:
            conn.rollback()


def test_get_users_and_orders_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("JoinLoc", 35))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("JoinLoc",))
    uid = cur.fetchone()[0]

    pub_name = f"JP-{random_suffix()}"
    auth_name = f"JA-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "jp@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("JG", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("JG",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "JoinBook", 2024, genre_id, pub_id, auth_id)
    )

    cur.execute("INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s,%s,CURRENT_DATE,%s)",
                (isbn, uid, 88.8))

    conn.commit()

    cur.execute("""
        SELECT u.location, o.price
        FROM users u
        JOIN orders o ON u.users_id = o.user_id
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
    conn.rollback()

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM book_genres WHERE genre = 'UG'")

        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("UG", 1))
        cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("UG",))
        gid = cur.fetchone()[0]

        cur.execute("UPDATE book_genres SET popularity = popularity + 5 WHERE id = %s", (gid,))

        cur.execute("SELECT popularity FROM book_genres WHERE id = %s", (gid,))
        assert cur.fetchone()[0] == 6

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise
    finally:
        try:
            cur.execute("DELETE FROM book_genres WHERE genre = 'UG'")
            conn.commit()
        except Exception:
            conn.rollback()


def test_update_user_location(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("OldLoc", 30))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("OldLoc",))
    uid = cur.fetchone()[0]
    conn.commit()

    new_loc = f"Loc-{random_suffix()}"
    cur.execute("UPDATE users SET location = %s WHERE users_id = %s", (new_loc, uid))
    conn.commit()

    cur.execute("SELECT location FROM users WHERE users_id = %s", (uid,))
    assert cur.fetchone()[0] == new_loc


def test_update_genre_popularity_group_by(conn):
    conn.rollback()

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM book_genres WHERE genre LIKE 'TestGenre_%'")

        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("TestGenre_1", 5))
        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("TestGenre_2", 3))
        cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("TestGenre_3", 7))

        cur.execute("""
            UPDATE book_genres 
            SET popularity = subquery.avg_popularity 
            FROM (
                SELECT AVG(popularity) as avg_popularity 
                FROM book_genres 
                WHERE genre LIKE 'TestGenre_%'
            ) as subquery 
            WHERE genre LIKE 'TestGenre_%'
        """)

        cur.execute("SELECT popularity FROM book_genres WHERE genre LIKE 'TestGenre_%'")
        results = cur.fetchall()

        expected_avg = 5.0
        for result in results:
            assert abs(result[0] - expected_avg) < 0.01, f"Oczekiwano {expected_avg}, otrzymano {result[0]}"

        assert len(results) == 3, f"Oczekiwano 3 rekordów, otrzymano {len(results)}"

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise
    finally:
        try:
            cur.execute("DELETE FROM book_genres WHERE genre LIKE 'TestGenre_%'")
            conn.commit()
        except Exception:
            conn.rollback()


def test_update_user_with_order_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("OldJoinLoc", 28))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("OldJoinLoc",))
    uid = cur.fetchone()[0]

    pub_name = f"PUJ-{random_suffix()}"
    auth_name = f"AUJ-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "puj@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("GJ", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("GJ",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "JoinBook2", 2024, genre_id, pub_id, auth_id)
    )

    cur.execute("INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s,%s,CURRENT_DATE,%s)",
                (isbn, uid, 77.7))

    conn.commit()

    new_loc = f"JoinLoc-{random_suffix()}"
    cur.execute("""
        UPDATE users
        SET location = %s
        FROM orders o
        WHERE users.users_id = o.user_id AND o.user_id = %s
    """, (new_loc, uid))
    conn.commit()

    cur.execute("SELECT location FROM users WHERE users_id = %s", (uid,))
    assert cur.fetchone()[0] == new_loc


# -----------------------
# DELETE tests
# -----------------------

def test_delete_genre_by_id(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("DG", 3))
    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("DG",))
    gid = cur.fetchone()[0]
    conn.commit()

    cur.execute("DELETE FROM book_genres WHERE id = %s", (gid,))
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM book_genres WHERE id = %s", (gid,))
    assert cur.fetchone()[0] == 0


def test_delete_user_by_id(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("DU", 25))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("DU",))
    uid = cur.fetchone()[0]
    conn.commit()

    cur.execute("DELETE FROM users WHERE users_id = %s", (uid,))
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM users WHERE users_id = %s", (uid,))
    assert cur.fetchone()[0] == 0


def test_delete_books_with_few_ratings_group_by(conn):
    cur = conn.cursor()
    pub_name = f"DP-{random_suffix()}"
    auth_name = f"DA-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "dp@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("DG", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("DG",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "DelBook", 2024, genre_id, pub_id, auth_id))

    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("DelLoc", 33))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("DelLoc",))
    uid = cur.fetchone()[0]

    cur.execute("INSERT INTO book_ratings (user_id, isbn, book_rating) VALUES (%s,%s,%s)", (uid, isbn, 5))

    conn.commit()

    cur.execute("""
        DELETE FROM books
        WHERE isbn IN (
            SELECT isbn FROM book_ratings GROUP BY isbn HAVING COUNT(*) < 2
        )
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM books WHERE isbn = %s", (isbn,))
    assert cur.fetchone()[0] == 0


def test_delete_orders_with_user_join(conn):
    cur = conn.cursor()
    cur.execute("INSERT INTO users (location, age) VALUES (%s,%s)", ("DelJoinLoc", 45))
    cur.execute("SELECT users_id FROM users WHERE location = %s", ("DelJoinLoc",))
    uid = cur.fetchone()[0]

    pub_name = f"DPJ-{random_suffix()}"
    auth_name = f"DAJ-{random_suffix()}"
    isbn = random_isbn()

    cur.execute("INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s,%s,%s,%s,%s)",
                (pub_name, "A", "PL", "dpj@mail", "000000"))

    cur.execute("INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s,%s,%s)",
                (auth_name, "US", "1970-01-01"))

    cur.execute("INSERT INTO book_genres (genre, popularity) VALUES (%s,%s)", ("DGJ", 1))

    cur.execute("SELECT publisher_id FROM publishers WHERE name = %s", (pub_name,))
    pub_id = cur.fetchone()[0]

    cur.execute("SELECT author_id FROM authors WHERE author_name = %s", (auth_name,))
    auth_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM book_genres WHERE genre = %s", ("DGJ",))
    genre_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s,%s,%s,%s,%s,%s)",
        (isbn, "DelJoinBook", 2024, genre_id, pub_id, auth_id))

    cur.execute("INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s,%s,CURRENT_DATE,%s)",
                (isbn, uid, 66.6))

    conn.commit()

    cur.execute("""
        DELETE FROM orders
        USING users u
        WHERE orders.user_id = u.users_id AND u.location = %s
    """, ("DelJoinLoc",))
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM orders WHERE user_id = %s", (uid,))
    assert cur.fetchone()[0] == 0


    # -----------------------
    # TEST START
    # -----------------------


def postgresql_tests():
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="my-secret-password",
        database="postgres",
        port="5432"
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

    print(f"Znaleziono {len(tests)} testów PostgreSQL")

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

                individual_results[data_size][test_name] = avg_time

                for crud_op, test_names in crud_categories.items():
                    if test_name in test_names:
                        crud_results[data_size][crud_op].append(avg_time)
                        break

        cleanup_test_data(conn)

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

    conn.close()
    return final_results


def prepare_test_data(conn, size):
    cursor = conn.cursor()
    try:
        conn.rollback()
        cleanup_test_data(conn)
        conn.commit()

        print(f"Przygotowywanie {size} rekordów danych testowych...")

        genres_data = []
        for i in range(20):
            genre_name = f"TestGenre_{i}"
            popularity = i % 10 + 1
            genres_data.append((genre_name, popularity))
        cursor.executemany(
            "INSERT INTO book_genres (genre, popularity) VALUES (%s, %s)",
            genres_data
        )

        publishers_data = []
        for i in range(50):
            name = f"TestPublisher_{i}"
            address = f"TestAddress_{i}"
            country = f"TestCountry_{i % 10}"
            mail = f"publisher{i}@test.com"
            phone = f"123-456-{i:04d}"
            publishers_data.append((name, address, country, mail, phone))
        cursor.executemany(
            "INSERT INTO publishers (name, address, country, mail, phone) VALUES (%s, %s, %s, %s, %s)",
            publishers_data
        )

        authors_data = []
        for i in range(100):
            author_name = f"TestAuthor_{i}"
            country_of_origin = f"TestCountry_{i % 20}"
            birth_date = f"19{50 + i % 50}-01-01"
            authors_data.append((author_name, country_of_origin, birth_date))
        cursor.executemany(
            "INSERT INTO authors (author_name, country_of_origin, birth_date) VALUES (%s, %s, %s)",
            authors_data
        )

        cursor.execute("SELECT id FROM book_genres WHERE genre LIKE 'TestGenre_%'")
        genre_ids = [row[0] for row in cursor.fetchall()]
        if not genre_ids:
            raise Exception("Nie udało się utworzyć gatunków testowych")

        cursor.execute("SELECT publisher_id FROM publishers WHERE name LIKE 'TestPublisher_%'")
        publisher_ids = [row[0] for row in cursor.fetchall()]
        if not publisher_ids:
            raise Exception("Nie udało się utworzyć wydawców testowych")

        cursor.execute("SELECT author_id FROM authors WHERE author_name LIKE 'TestAuthor_%'")
        author_ids = [row[0] for row in cursor.fetchall()]
        if not author_ids:
            raise Exception("Nie udało się utworzyć autorów testowych")

        users_data = []
        for i in range(size):
            location = f"TestCity_{i % 100}"
            age = 18 + (i % 62)
            users_data.append((location, age))
        cursor.executemany(
            "INSERT INTO users (location, age) VALUES (%s, %s)",
            users_data
        )

        books_data = []
        cursor.execute(
            "SELECT COALESCE(MAX(CAST(SUBSTRING(isbn FROM 6) AS INTEGER)), -1) FROM books WHERE isbn LIKE 'TEST-%'"
        )
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
            "INSERT INTO books (isbn, book_name, year_of_release, genre_id, publisher_id, author_id) VALUES (%s, %s, %s, %s, %s, %s)",
            books_data
        )

        cursor.execute("SELECT users_id FROM users WHERE location LIKE 'TestCity_%'")
        user_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT isbn FROM books WHERE book_name LIKE 'TestBook_%'")
        book_isbns = [row[0] for row in cursor.fetchall()]

        ratings_data = []
        for i in range(min(size, len(user_ids) * len(book_isbns) // 10)):
            user_id = user_ids[i % len(user_ids)]
            isbn = book_isbns[i % len(book_isbns)]
            rating = (i % 5) + 1
            ratings_data.append((user_id, isbn, rating))
        if ratings_data:
            cursor.executemany(
                "INSERT INTO book_ratings (user_id, isbn, book_rating) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                ratings_data
            )

        orders_data = []
        for i in range(size // 2):
            isbn = book_isbns[i % len(book_isbns)]
            user_id = user_ids[i % len(user_ids)]
            order_date = f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
            price = round(10.0 + (i % 50), 2)
            orders_data.append((isbn, user_id, order_date, price))
        if orders_data:
            cursor.executemany(
                "INSERT INTO orders (isbn, user_id, order_date, price) VALUES (%s, %s, %s, %s)",
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
        conn.rollback()

        cursor.execute(
            "DELETE FROM returns WHERE order_id IN (SELECT order_id FROM orders WHERE user_id IN (SELECT users_id FROM users WHERE location LIKE 'TestCity_%'))")
        cursor.execute(
            "DELETE FROM orders WHERE user_id IN (SELECT users_id FROM users WHERE location LIKE 'TestCity_%')")
        cursor.execute(
            "DELETE FROM book_ratings WHERE isbn IN (SELECT isbn FROM books WHERE book_name LIKE 'TestBook_%')")
        cursor.execute("DELETE FROM books WHERE book_name LIKE 'TestBook_%'")
        cursor.execute("DELETE FROM users WHERE location LIKE 'TestCity_%'")
        cursor.execute("DELETE FROM authors WHERE author_name LIKE 'TestAuthor_%'")
        cursor.execute("DELETE FROM publishers WHERE name LIKE 'TestPublisher_%'")
        cursor.execute("DELETE FROM book_genres WHERE genre LIKE 'TestGenre_%'")

        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM book_genres")
        max_genre_id = cursor.fetchone()[0]
        cursor.execute(f"SELECT setval('book_genres_id_seq', {max_genre_id + 1}, false)")

        cursor.execute("SELECT COALESCE(MAX(users_id), 0) FROM users")
        max_user_id = cursor.fetchone()[0]
        cursor.execute(f"SELECT setval('users_users_id_seq', {max_user_id + 1}, false)")

        cursor.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
        max_order_id = cursor.fetchone()[0]
        cursor.execute(f"SELECT setval('orders_order_id_seq', {max_order_id + 1}, false)")

        try:
            cursor.execute("SELECT COALESCE(MAX(return_id), 0) FROM returns")
            max_return_id = cursor.fetchone()[0]
            cursor.execute(f"SELECT setval('returns_return_id_seq', {max_return_id + 1}, false)")
        except Exception:
            pass

        conn.commit()
        print("Dane testowe wyczyszczone i sekwencje zsynchronizowane")
    except Exception as e:
        print(f"Błąd podczas czyszczenia danych: {e}")
        conn.rollback()
    finally:
        cursor.close()



