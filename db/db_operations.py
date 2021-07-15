from sqlite3 import connect, Error
import os
from sqlite3.dbapi2 import OperationalError, ProgrammingError
from typing import Final
from termcolor import colored

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)


class DbOperator:
    def __init__(self, **kwargs) -> None:
        # The DB_NAME is initialized as a final obj to show that its a conctant
        self.DB_NAME: Final = 'db.sqlite3'
        self.connection = None

    def create_connection(self):
        """ create a database connection to a SQLite database in my file system"""
        db_path = os.path.join(BASE_DIR, self.DB_NAME)
        try:
            self.connection = connect(str(db_path))

        except ProgrammingError as e:
            raise ProgrammingError(e)

        except Exception as e:
            raise os.error(e)

        return self.connection

    def create_conn_in_RAM(self):
        """ 
        create a database connection to a database that resides
        in the memory
        """
        try:
            with open("db_path", "w+") as db:
                self.connection = connect(":memory:")

        except Error as e:
            raise ConnectionError(e)

        except Exception as e:
            raise os.error(e)
        return self.connection

    def create_table(self, create_table_schema):
        """
         create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(create_table_schema)

        except Error as e:
            raise ProgrammingError(e)

        except Exception as e:
            raise os.error(e)

        finally:
            self.connection.close()

    def insert_data(self, sql: str, table: str):
        """
        Create a new project into the <<table>> table
        :param conn:
        :param <<table>>:
        :return: <<table>> id
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(sql)
                self.connection.commit()

                return self.cursor.lastrowid

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        finally:
            self.connection.close()

    def fetch_all_objects(self, table):
        """
        Query all rows in the <<table>> table
        :return: all rows
        """

        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(f"SELECT * FROM {table}")
                rows = self.cursor.fetchall()
                return rows

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        except Error as e:
            raise Error(e)

        except Exception as e:
            raise Exception(e)

    def fetch_object_by_priority(self, table, id):
        """
        Query tasks by priority
        :param conn: the Connection object
        :param priority:
        :return:
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(
                    f"SELECT * FROM {table} WHERE id={id}")
                rows = self.cursor.fetchall()
                return rows

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        except Error as e:
            raise Error(e)

        except Exception as e:
            raise Exception(e)

    def update_object(self, sql, values):
        """
        update values in db
        :param conn:
        :param task:
        :return: project id
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(sql, values)
                self.connection.commit()

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        except Error as e:
            raise Error(e)

        except Exception as e:
            raise Exception(e)

    def del_all_from_table(self, table):
        """
        Delete all rows in the <<table>>
        :param conn: Connection to the SQLite database
        :return:
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(f'DELETE FROM {table}')
                self.connection.commit()

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        except Error as e:
            raise Error(e)

        except Exception as e:
            raise Exception(e)

    def del_one_from_table(self, id, table):
        """
        Delete one row in the <<table>>
        :param conn: Connection to the SQLite database
        :return:
        """
        try:
            self.connection = self.create_connection()
            with self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(f'DELETE FROM {table} WHERE id=?', (id,))
                self.connection.commit()

        except ProgrammingError as e:
            raise Error(e)

        except OperationalError as e:
            raise OperationalError(e)

        except Error as e:
            raise Error(e)

        except Exception as e:
            raise Exception(e)
