
-- #1 query

SELECT c.name,
       count(fc.film_id) AS number_of_films
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.category_id
ORDER BY number_of_films DESC

-- #2 query

-- SELECT act.first_name,
--        act.last_name,
--        sum(f.rental_duration*f.rental_rate) AS rental_ammount
-- FROM film f
-- JOIN film_actor fa USING(film_id)
-- JOIN actor act USING(actor_id)
-- GROUP BY act.actor_id
-- ORDER BY rental_ammount DESC
-- LIMIT 10


-- #3 query

-- SELECT c.name,
--        sum(f.replacement_cost) AS total_cost
-- FROM film f
-- JOIN film_category fc USING(film_id)
-- JOIN category c USING(category_id)
-- GROUP BY c.category_id
-- ORDER BY total_cost DESC
-- LIMIT 1


-- #4 query

-- SELECT film.title
-- FROM film
-- LEFT JOIN inventory USING(film_id)
-- WHERE inventory_id IS NULL


-- #5 query

-- SELECT act.first_name,
--        act.last_name,
--        count(fa.film_id) AS number_of_children_films
-- FROM film f
-- JOIN film_actor fa  using(film_id)
-- JOIN actor act  using(actor_id)
-- JOIN film_category fc  using(film_id)
-- JOIN category cat  using(category_id)
-- WHERE cat.name = 'Children'
-- GROUP BY act.actor_id
-- ORDER BY number_of_children_films DESC
-- LIMIT 5
-- вывел 5, потому что у 4 были одинаковые показатели


-- #6 query

-- SELECT ci.city,
--        sum(cust.active) over(PARTITION BY ci.city) AS active,
--        sum(abs(cust.active - 1)) OVER (PARTITION BY ci.city) AS inactive
-- FROM city ci
-- JOIN address ad USING(city_id)
-- JOIN customer cust USING(address_id)
-- ORDER BY inactive DESC

-- #7 query

-- (SELECT category.name,
--           EXTRACT(epoch
--                   FROM sum(rental.return_date - rental.rental_date))/3600 AS rental_period
--    FROM film
--    JOIN film_category USING(film_id)
--    JOIN category USING(category_id)
--    JOIN inventory USING(film_id)
--    JOIN rental USING(inventory_id)
--    JOIN customer USING(customer_id)
--    JOIN address USING(address_id)
--    JOIN city USING(city_id)
--    WHERE film.title Like 'A%'
--      AND rental.return_date IS NOT NULL
--    GROUP BY category.category_id,
--             city.city
--    ORDER BY rental_period DESC
--    LIMIT 1)
-- UNION
--   (SELECT category.name,
--           sum(EXTRACT(epoch
--                       FROM(rental.return_date - rental.rental_date))/3600) AS rental_period
--    FROM film
--    JOIN film_category USING(film_id)
--    JOIN category USING(category_id)
--    JOIN inventory USING(film_id)
--    JOIN rental USING(inventory_id)
--    JOIN customer USING(customer_id)
--    JOIN address USING(address_id)
--    JOIN city USING(city_id)
--    WHERE city.city Like '%-%'
--      AND rental.return_date IS NOT NULL
--    GROUP BY category.category_id,
--             city.city
--    ORDER BY rental_period DESC
--    LIMIT 1)


