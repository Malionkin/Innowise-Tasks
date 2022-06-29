COUNT_STUDENTS_QR = """
SELECT
    r.name,
    COUNT(s.id)
FROM
    hostel.rooms r
JOIN hostel.students s
ON
    r.id = s.room
GROUP BY
    r.id;"""

FIVE_YOUNG_QR = """
SELECT
    s.room,
    AVG(
        (
            (
                YEAR(CURRENT_DATE) - YEAR(s.birthday)
            ) -(
                DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d')
            )
        )
    ) AS avg_age
FROM
    hostel.rooms r
JOIN hostel.students s
ON
    r.id = s.room
GROUP BY
    r.id
ORDER BY
    avg_age
LIMIT 5;"""

BIGGEST_DELTA_QR = """
SELECT
    r.name,
    MAX(
        (
            (
                (
                    YEAR(CURRENT_DATE) - YEAR(s.birthday)
                ) -(
                    DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d')
                )
            )
        )
    ) - MIN(
        (
            (
                (
                    YEAR(CURRENT_DATE) - YEAR(s.birthday)
                ) -(
                    DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d')
                )
            )
        )
    ) AS delta
FROM
    hostel.rooms r
JOIN hostel.students s
ON
    r.id = s.room
GROUP BY
    r.id
ORDER BY
    delta
DESC
    ,
    r.id
LIMIT 5;"""

DIFFERENT_SEX_QR = """
SELECT
    r.name,
    COUNT(DISTINCT s.sex) AS number_of_genders
FROM
    hostel.rooms r
JOIN hostel.students s
ON
    r.id = s.room
GROUP BY
    s.room
HAVING
    COUNT(DISTINCT s.sex) > 1;"""