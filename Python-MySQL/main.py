import json
import pymysql
import simplejson

rooms_data = open("rooms.json").read()
rooms = json.loads(rooms_data)
students_data = open("students.json").read()
students = json.loads(students_data)

con = pymysql.connect(host="localhost", user="root", password="", db="hostel")

class DataBase():

    def fill_data(self):
        cursor = con.cursor()
        for i in rooms:
            id = i.get("id")
            name = i.get("name")
            cursor.execute("insert into rooms(id, name) value(%s,%s)", (id, name))
        con.commit()

        for i in students:
            bday = i.get("birthday")
            id = i.get("id")
            name = i.get("name")
            room = i.get("room")
            sex = i.get("sex")
            cursor.execute("insert into students(birthday, id, name, room, sex) value(%s,%s,%s,%s,%s)",
                           (bday, id, name, room, sex))
        con.commit()

    def rooms_and_number_of_students(self):
        with con.cursor() as cursor:
            rooms_and_number_of_students = \
                "select r.name, count(s.id) " \
                "from hostel.rooms r join hostel.students s " \
                "on r.id = s.room " \
                "group by r.id;"
            cursor.execute(rooms_and_number_of_students)
            rows = cursor.fetchall()
            result = dict(rows)

            with open('count_students_in_room.json', 'w') as f:
                simplejson.dump(result, f, indent=4)
            con.commit()

    def five_young(self):
        with con.cursor() as cursor:
            age = \
                "select" \
                " s.room ," \
                " avg(((YEAR(CURRENT_DATE) - YEAR(s.birthday)) - " \
                "(DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d')))) AS avg_age" \
                " from hostel.rooms r join hostel.students s " \
                "on r.id = s.room " \
                "group by r.id " \
                "order by avg_age" \
                " limit 5;"

            cursor.execute(age)
            rows = cursor.fetchall()
            result = dict(rows)

            with open('top_5_avg_age.json', 'w') as f:
                simplejson.dump(result, f, indent=4)
            con.commit()

    def biggest_delta(self):
        with con.cursor() as cursor:
            delta = "select r.name," \
                    " max((((YEAR(CURRENT_DATE) - YEAR(s.birthday)) - " \
                    "(DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d'))))) - " \
                    "min((((YEAR(CURRENT_DATE) - YEAR(s.birthday)) - " \
                    "(DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(s.birthday, '%m%d'))))) as delta " \
                    "from hostel.rooms r join hostel.students s on r.id = s.room " \
                    "group by r.id " \
                    "order by delta desc, r.id " \
                    "limit 5;"

            cursor.execute(delta)
            rows = cursor.fetchall()
            result = dict(rows)
            with open('biggest_delta.json', 'w') as f:
                simplejson.dump(result, f, indent=4)
            con.commit()

    def different_sex_rooms(self):
        with con.cursor() as cursor:
            different_sex = \
                "select" \
                " r.name, count(DISTINCT s.sex) AS number_of_genders" \
                " from hostel.rooms r join hostel.students s " \
                "on r.id = s.room " \
                "group by s.room " \
                "having count(DISTINCT s.sex) > 1;"

            cursor.execute(different_sex)
            rows = cursor.fetchall()
            result = dict(rows)

            with open('different_sex.json', 'w') as f:
                simplejson.dump(result, f, indent=4)
            con.commit()


hostel = DataBase()

hostel.different_sex_rooms()
hostel.rooms_and_number_of_students()
hostel.biggest_delta()
hostel.five_young()


