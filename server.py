from flask import Flask, request, jsonify
import pymysql
from flask_cors import CORS 

app = Flask(__name__)
CORS(app) 

@app.route("/movies")
def movies():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    with db.cursor() as cursor:
        sql = """Select f.film_id, f.title, f.rating, f.rental_duration, 
                f.rental_rate, count(f.film_id) as rent_count, f.description, f.length,
                f.release_year, f.replacement_cost, c.name,
                (SELECT GROUP_CONCAT(a.first_name, ' ', a.last_name SEPARATOR ', ')
                    FROM actor a
                    JOIN film_actor fa ON a.actor_id = fa.actor_id
                    WHERE fa.film_id = f.film_id) as actors
                    from film f
                    left join film_category fc on fc.film_id = f.film_id
                    left join category c on c.category_id = fc.category_id
                    left join inventory i on i.film_id = f.film_id
                    left join rental r on r.inventory_id = i.inventory_id
                where r.rental_date IS NOT NULL
                GROUP BY f.film_id, f.title, c.name
                order by count(f.film_id) desc;"""
        cursor.execute(sql)
        results = cursor.fetchall()
        if not results:
            return jsonify({"films": []})
        return jsonify({"films": results})
    

@app.route("/categories")
def categories():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    with db.cursor() as cursor:
        sql = """Select name
                FROM category
                ORDER BY name ASC;"""
        cursor.execute(sql)
        results = cursor.fetchall()
        if not results:
            return jsonify({"categories": []})
        return jsonify({"categories": results})



@app.route("/topMovies")
def topMovies():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    with db.cursor() as cursor:
        sql = """Select f.film_id, f.title, f.rating, f.rental_duration, 
                f.rental_rate, count(f.film_id) as rent_count, f.description, f.length,
                f.release_year, f.replacement_cost
                    from film f
                    left join film_category fc on fc.film_id = f.film_id
                    left join category c on c.category_id = fc.category_id
                    left join inventory i on i.film_id = f.film_id
                    left join rental r on r.inventory_id = i.inventory_id
                where r.rental_date IS NOT NULL
                GROUP BY f.film_id, f.title, c.name
                order by count(f.film_id) desc
                LIMIT 5"""
        cursor.execute(sql)
        results = cursor.fetchall()
        return jsonify({"films": results})




@app.route("/topActors")
def topActors():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    with db.cursor() as cursor:
        sql = """Select a.actor_id, a.first_name, a.last_name, count(f.film_id) as movies
                from film f
                    left join film_actor fa on fa.film_id = f.film_id
                    left join actor a on a.actor_id = fa.actor_id
                where a.actor_id IS NOT NULL
                group by a.actor_id, a.first_name, a.last_name
                order by count(f.film_id) desc
                LIMIT 5;"""
        cursor.execute(sql)
        results = cursor.fetchall()
        return jsonify({"actors": results})



@app.route("/topMoviesForActor")
def topMoviesForActor():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    actorID = request.args.get('actorId')
    with db.cursor() as cursor:
        sql = """SELECT f.film_id, f.title
                 FROM film f
                 LEFT JOIN film_category fc ON fc.film_id = f.film_id
                 LEFT JOIN category c ON c.category_id = fc.category_id
                 LEFT JOIN inventory i ON i.film_id = f.film_id
                 LEFT JOIN rental r ON r.inventory_id = i.inventory_id
                 LEFT JOIN film_actor fa ON fa.film_id = f.film_id
                 WHERE r.rental_date IS NOT NULL
                   AND fa.actor_id = %s
                 GROUP BY f.film_id, f.title, c.name
                 ORDER BY COUNT(f.film_id) DESC
                 LIMIT 5;"""
        cursor.execute(sql, (actorID,))
        results = cursor.fetchall()
        return jsonify({"films": results})





@app.route("/filmactor")
def actors():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    with db.cursor() as cursor:
        sql = "SELECT film_id, FROM film_actor"
        cursor.execute(sql)
        results = cursor.fetchall()
        return jsonify({"filmactor": results})
    


@app.route("/customers", methods=['POST'])
def customer():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    data = request.get_json()
    customerID = data.get('customer_id')
    with db.cursor() as cursor:
        query = "SELECT * FROM customer WHERE customer_id = %s"
        cursor.execute(query, ([customerID]))
        results = cursor.fetchall()
        return jsonify({"customer": results})


@app.route("/fhfs", methods=['POST'])
def create_customer():
    db = pymysql.connect(host="localhost", user="root", password="!@Jff05288", db="sakila")
    data = request.get_json()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    phone_number = data.get('phone_number')
    address = data.get('address')
    city = data.get('city')
    state = data.get('state')
    country = data.get('country')
    postalCode = data.get('postalCode')
    credit_card_number = data.get('credit_card_number')
    security_code = data.get('security_code')
    movie = data.get('movie')
    cost = data.get('cost')
    
    

    countryID = 0
    cityID = 0
    addressID = 0
    customerID = 0
    filmID = 0
    inventoryID = 0
    rentalID = 0
    paymentID = 0

    #get countryID
    with db.cursor() as cursor:
        query = "SELECT * FROM country WHERE country = %s"
        cursor.execute(query, ([country]))
        results = cursor.fetchall()
        if results:
            countryID = results[0][0]
        else:
            query = """
                INSERT INTO country (country, last_update)
                VALUES (%s, NOW())
                """
            cursor.execute(query, ([country]))
            db.commit()
            query = "SELECT * FROM country WHERE country = %s"
            cursor.execute(query, ([country]))
            results = cursor.fetchall()
            countryID = results[0][0]



    #get cityID
    with db.cursor() as cursor:
        query = "SELECT * FROM city WHERE city = %s"
        cursor.execute(query, (city))
        results = cursor.fetchall()
        if results:
            cityID = results[0][0]
        else:
            query = """
                INSERT INTO city (city, country_id, last_update)
                VALUES (%s, %s, NOW())
                """
            cursor.execute(query, (city, countryID))
            db.commit()
            query = "SELECT * FROM city WHERE city = %s"
            cursor.execute(query, (city))
            results = cursor.fetchall()
            cityID = results[0][0]


    

    #get addressID
    with db.cursor() as cursor:
        query = "SELECT * FROM address WHERE address = %s"
        cursor.execute(query, [address])
        results = cursor.fetchall()
        if results:
            addressID = results[0][0]
        else:
            query = """
                INSERT INTO address (address, address2, district, 
                        city_id, postal_code, phone, last_update)
                VALUES (%s, NULL, %s, %s, %s, %s, NOW())
                """
            cursor.execute(query, ([address], '', cityID, postalCode,
                                   phone_number))
            db.commit()
            query = "SELECT * FROM address WHERE address = %s"
            cursor.execute(query, ([address]))
            results = cursor.fetchall()
            addressID = results[0][0]



    # get customerID
    with db.cursor() as cursor:
        query = "SELECT * FROM customer WHERE email = %s"
        cursor.execute(query, ([email]))
        results = cursor.fetchall()
        if results:
            customerID = results[0][0]
        else:
            query = """
                INSERT INTO customer (store_id, first_name, last_name, 
                        email, address_id, active, create_date, last_update)
                VALUES (1, %s, %s, %s, %s, 1, NOW(), NOW())
                """
            cursor.execute(query, (first_name, last_name, [email], addressID))
            db.commit()
            query = "SELECT * FROM customer WHERE email = %s"
            cursor.execute(query, ([email]))
            results = cursor.fetchall()
            customerID = results[0][0]





    # get filmID
    with db.cursor() as cursor:
        query = "SELECT * FROM film WHERE title = %s"
        cursor.execute(query, ([movie]))
        results = cursor.fetchall()
        filmID = results[0][0]


    


    # get inventoryID
    with db.cursor() as cursor:
        query = "SELECT * FROM inventory WHERE film_id = %s"
        cursor.execute(query, (filmID))
        results = cursor.fetchall()
        inventoryID = results[0][0]
       




    # get rentalID
    with db.cursor() as cursor:
        query = """
            INSERT INTO rental (rental_date, inventory_id, customer_id, 
                            return_date, staff_id, last_update)
            VALUES (NOW(), %s, %s, NULL, 1, NOW())
            """
        cursor.execute(query, (inventoryID, customerID))
        db.commit()
        query = """SELECT * FROM rental WHERE inventory_id = %s
                    AND customer_id = %s ORDER BY rental_id desc"""
        cursor.execute(query, (inventoryID, customerID))
        results = cursor.fetchall()
        rentalID = results[0][0]



    # insert payment
    with db.cursor() as cursor:
        query = """
            INSERT INTO payment (customer_id, staff_id, rental_id, 
                            amount, payment_date, last_update)
            VALUES (%s, 1, %s, %s, NOW(), NOW())
            """
        cursor.execute(query, (customerID, rentalID, [cost]))
        db.commit()
        query = """SELECT * FROM payment WHERE rental_id = %s"""
        cursor.execute(query, (rentalID))
        results = cursor.fetchall()
        paymentID = results[0][0]

    
        


    return jsonify({'message': 'Customer created'}), 201

if __name__ == "__main__":
    app.run(debug=True)
