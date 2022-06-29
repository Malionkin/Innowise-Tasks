import json
import pymysql
import simplejson
from quiries import main_quiries as mq

with open("rooms.json") as rooms_data:
    rooms = json.loads(rooms_data.read())

with open("students.json") as students_data:
    students = json.loads(students_data.read())



class DataBase():
    con = pymysql.connect(host="localhost", user="root", password="", db="hostel")

    def fill_data(self):
        cursor = DataBase.con.cursor()
        for i in rooms:
            id = i.get("id")
            name = i.get("name")
            cursor.execute("insert into rooms(id, name) value(%s,%s)", (id, name))
        DataBase.con.commit()

        for i in students:
            bday = i.get("birthday")
            id = i.get("id")
            name = i.get("name")
            room = i.get("room")
            sex = i.get("sex")
            cursor.execute("insert into students(birthday, id, name, room, sex) value(%s,%s,%s,%s,%s)",
                           (bday, id, name, room, sex))
        DataBase.con.commit()


    def rooms_and_number_of_students_query(self):
        with DataBase.con.cursor() as cursor:
            rooms_and_number_of_students = mq.COUNT_STUDENTS_QR
            cursor.execute(rooms_and_number_of_students)
            rows = cursor.fetchall()
        return dict(rows)

    def load_rooms_and_number_of_students_query(self):
        with open('count_students_in_room.json', 'w') as f:
            simplejson.dump(self.rooms_and_number_of_students_query(), f, indent=4)
            DataBase.con.commit()

    def five_young_query(self):
        with DataBase.con.cursor() as cursor:
            age = mq.FIVE_YOUNG_QR
            cursor.execute(age)
            rows = cursor.fetchall()
        return dict(rows)

    def load_five_young_query(self):
        with open('top_5_avg_age.json', 'w') as f:
            simplejson.dump(self.five_young_query(), f, indent=4)
            DataBase.con.commit()

    def biggest_delta_query(self):
        with DataBase.con.cursor() as cursor:
            delta = mq.BIGGEST_DELTA_QR
            cursor.execute(delta)
            rows = cursor.fetchall()
        return dict(rows)

    def load_biggest_delta_query(self):
        with open('biggest_delta.json', 'w') as f:
            simplejson.dump(self.biggest_delta_query(), f, indent=4)
            DataBase.con.commit()

    def different_sex_rooms_query(self):
        with DataBase.con.cursor() as cursor:
            different_sex = mq.DIFFERENT_SEX_QR
            cursor.execute(different_sex)
            rows = cursor.fetchall()
            return dict(rows)

    def load_diff_sex_query(self):
        with open('different_sex.json', 'w') as f:
            simplejson.dump(self.different_sex_rooms_query(), f, indent=4)
            DataBase.con.commit()







hostel = DataBase()

hostel.load_diff_sex_query()
hostel.load_rooms_and_number_of_students_query()
hostel.load_biggest_delta_query()
hostel.load_five_young_query()


