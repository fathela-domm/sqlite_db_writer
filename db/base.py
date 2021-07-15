from backend.db.db_operations import DbOperator
import traceback
from termcolor import colored

# The global object below will be used to transfer data to the db
global EXTERN_SQL_DB_SCHEMA, CREATE_DB_TABLE_SCHEMA
EXTERN_SQL_DB_SCHEMA: dict = {}
CREATE_DB_TABLE_SCHEMA: dict = {}
DATA_TO_INSERT: dict = {}


class BASE_MODEL(DbOperator):
    def __init__(self) -> None:
        super().__init__()
        self.DB_TABLE_NAME: str = self.__class__.__name__
        # Creating the db table if it does not exists
        self.create_db_table()

    def create_db_table(self) -> str:
        print(colored("[+++]DB table created: ", "green"))
        return super().create_table(
            f"""-- {self.DB_TABLE_NAME} table
                CREATE TABLE IF NOT EXISTS {self.DB_TABLE_NAME} (
{''.join(f"{key} {value}"  for key, value in EXTERN_SQL_DB_SCHEMA.items())}id integer PRIMARY KEY
                );
            """
        )

    def create(self, **kwargs) -> None:
        # return sql statement to insert data in db
        if kwargs.keys() != EXTERN_SQL_DB_SCHEMA.keys():
            raise ValueError(
                "Invalid db insert query made for table " + self.DB_TABLE_NAME
            )

        super().insert_data(
            f'''
                    INSERT INTO {self.DB_TABLE_NAME}{tuple(kwargs.keys())}
                    VALUES{tuple(kwargs.values())}
            ''',
            self.DB_TABLE_NAME,
        )
        # print(
        #     colored(
        #         "\t\t[+++]Data successfully inserted into table " +
        #         self.DB_TABLE_NAME, "green"
        #     )
        # )
        return self.all()

    def all(self):
        # This method will return all the stored values in the db as a queryset for a table
        return super().fetch_all_objects(self.DB_TABLE_NAME)

    def filter(self, pk=None):
        # return sql statemnt to filter data in db
        if not pk:
            raise ValueError("[!!]Argument id not provided")
        return super().fetch_object_by_priority(self.DB_TABLE_NAME, pk)

    def update(self, pk=None, **options):
        # return sql statemnt to update data in db
        if not pk:
            raise ValueError("[!!]Argument id not provided")

        sql = f'''
        UPDATE {self.DB_TABLE_NAME}
            SET id={pk},''' + '\n' + ''.join('\t\t%s = ?,\n' % (key) for key in options.keys()) + '\t\t' + f'''id={pk}
            WHERE id = {pk}
        '''
        super().update_object(sql, tuple(options.values()))
        return self.filter(pk=pk)

    def delete_one(self, pk=None):
        if not pk:
            raise ValueError("[!!]Argument id not provided")
        super().del_one_from_table(pk, self.DB_TABLE_NAME)
        return self.all()

    def delete_all(self):
        super().del_all_from_table(self.DB_TABLE_NAME)
        return self.all()

    class CharField:
        def __init__(self, null=False, blank=False, max_length=None, object_name=None, **kwargs) -> None:
            if max_length:
                self.max_length = max_length
                self.null: bool = null
                self.blank: bool = blank

                # detemining the object name if not alredy defined
                if object_name == None:
                    (filename, line_number, function_name,
                        text) = traceback.extract_stack()[-2]
                    object_name = text[:text.find('=')].strip()
                    self.defined_name = object_name

                self.to_dict()

            else:
                raise ValueError(
                    colored("A required kwarg max_length was not passed", "red")
                )

        @ property
        def characterFieldSql(self) -> str:
            if not self.null and not self.blank:
                return f"VARCHAR({int(self.max_length)}),\n"

            elif self.null:
                return f"VARCHAR({int(self.max_length)}) NULL,\n"

            else:
                return f"VARCHAR({int(self.max_length)}) NOT NULL,\n"

        def to_dict(self) -> dict:
            return EXTERN_SQL_DB_SCHEMA.update({str(self.defined_name): self.characterFieldSql})

    class IntegerField:
        def __init__(self, null=False, blank=False, object_name=None, **kwargs) -> None:
            self.null: bool = null
            self.blank: bool = blank

            # detemining the object name if not alredy defined
            if object_name == None:
                (filename, line_number, function_name,
                    text) = traceback.extract_stack()[-2]
                object_name = text[:text.find('=')].strip()
                self.defined_name = object_name
            self.to_dict()

        @ property
        def integerFieldSql(self):
            if self.null:
                return "integer NULL,\n"

            else:
                return "integer NOT NULL,\n"

        def to_dict(self) -> dict:
            return EXTERN_SQL_DB_SCHEMA.update({str(self.defined_name): self.integerFieldSql})

    class DateTimeField(CharField):
        def __init__(self,  null=False, blank=False, object_name=None, auto_now_add=False) -> None:
            if object_name == None:
                (filename, line_number, function_name,
                    text) = traceback.extract_stack()[-2]
                object_name = text[:text.find('=')].strip()
                self.defined_name = object_name

            # The parameter below will determine date update on change of a value in the db or not
            self.auto_now_add = auto_now_add
            super().__init__(
                null=null,
                blank=blank,
                object_name=self.defined_name,
                max_length=10
            )

        # In the method below we override the base class to_dict() method to return a date rather than mere text
        def to_dict(self) -> dict:
            return EXTERN_SQL_DB_SCHEMA.update({str(self.defined_name): super().characterFieldSql})

    class FloatField:
        def __init__(self, null=False, blank=False, object_name=None, **kwargs) -> None:
            self.null: bool = null
            self.blank: bool = blank

            if object_name == None:
                (filename, line_number, function_name,
                    text) = traceback.extract_stack()[-2]
                object_name = text[:text.find('=')].strip()
                self.defined_name = object_name
            self.to_dict()

        @ property
        def floatFieldSql(self):
            if self.null:
                return f"real NULL,\n"
            else:
                return f"real NULL,\n"

        def to_dict(self) -> dict:
            return EXTERN_SQL_DB_SCHEMA.update({str(self.defined_name): self.floatFieldSql})

    class DecimalField:
        def __init__(self, max_digits=None, decimal_places=None, null=False, blank=False, object_name=None) -> None:
            if max_digits or decimal_places:
                self.null: bool = null
                self.blank: bool = blank
                self.max_digits = max_digits
                self.decimal_places = decimal_places

                if object_name == None:
                    (filename, line_number, function_name,
                        text) = traceback.extract_stack()[-2]
                    object_name = text[:text.find('=')].strip()
                    self.defined_name = object_name
                self.to_dict()

            else:
                raise ValueError(
                    colored("A required kwarg max_length was not passed", "red")
                )

        @ property
        def decimalFieldSql(self):
            if self.null:
                return f"DECIMAL({self.max_digits},{self.decimal_places}) NULL,\n"
            else:
                return f"DECIMAL({self.max_digits},{self.decimal_places}) NOT NULL,\n"

        def to_dict(self) -> dict:
            return EXTERN_SQL_DB_SCHEMA.update({str(self.defined_name): self.decimalFieldSql})
