import sqlite3
import os
import shutil
from datetime import datetime

# from datetime import date


PATH = "./rutinas/"
# this space is for data base


def create_db(name=str):
    """Crea la base de datos alojada en el PATH establecido en la variable 'PATH', simplemente arroja el nombre, ya que el motor de DB es sqlite"""
    db = f"{PATH}{name}.db"
    if not os.path.exists(db):
        conn = sqlite3.connect(db)
        conn.close()
        print(f"{name} se ha crado con exito")
    else:
        print(f"¡Ya EXISTE: {name}!")


def list_db():
    """Lista todas las bases de datos situadas en el PATH, simplemente arroja todos los archivos que se encuentren en el, si existen otro tipo de archivos que no coincidan con el '.db' igual seran listados"""
    dbs = os.listdir(PATH)
    for db in dbs:
        print(db)
    print(f"Listado de {len(dbs)} exitoso")


def rename_db(name=str, new_name=str):
    """Renombra la base de datos deseada, simplemente escribiendo el nombre de la DB anterior y escribiendo el nuevo posteriormente a este"""
    try:
        shutil.move(f"{PATH}{name}.db", f"{PATH}{new_name}.db")
    except FileNotFoundError:
        print(f"¡No existe y no se puede ACTUALIZAR: {name}!, ")


def delete_db(name=str):
    """Elimina la DB deseada, simplemente escribiendo el nombre, y esta la borrara del PATH"""
    try:
        os.remove(f"{PATH}{name}.db")
    except FileNotFoundError:
        print(f"¡No existe y no se puede BORRAR: {name}!")
    else:
        print(f"¡Se ha BORRADO: {name}!")


# this space is for tables and more tables


def create_table(db_name=str, table_name=str, num_column=1):
    """Para crear la tabla necesitas introducir el nombre de la base de datos a la cual vas a acceder y despues ingresar el nombre de la tabla a crear y por el ultimo el numero de columnas extras que contendra, esta tabla tendra por defecto la columna de 'id' y de 'date' asi que el numero que agregues sera un extra"""
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, date TEXT"

        for i in range(num_column):
            sentence += f"Serie{i+1} TEXT, "

        sentence = sentence.rstrip(", ")
        sentence += ");"
        cursor.execute(sentence)
        conn.commit()
    except Exception as e:
        print(f"Uy, parece que ha ocurrido un error, investiga: {e}")
    else:
        print(f"La tabla: {table_name} ha sido CREADA con exito")
    finally:
        conn.close()


def read_table(db_name, table_name):
    """Para leer todo lo contenido en la tabla, solamente con introducir la DB y el nombre de la tabla listar"""
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = f"SELECT * FROM {table_name};"
        cursor.execute(sentence)
        rows = cursor.fetchall()
        conn.commit()
    except Exception:
        print("Uy, parece que ha ocurrido un error, investiga")
    else:
        print(f"{db_name}, {table_name}: {rows}")
    finally:
        conn.close()


def list_tables(db_name):
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = "SELECT name FROM sqlite_master WHERE type='table';"
        cursor.execute(sentence)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Uy, parece que ha ocurrido un error, investiga: {e}")
    else:
        print(f"Tablas listadas: {rows}")
    finally:
        conn.close()


# Update Database


def rename_table(db_name, table_name, new_table_name):
    """Si es necesario renombrar la tabla, primero se necesita localizar la DB en el path, nombrarla, despues escribir el nombre de la tabla y despues escribir el nuevo nombre de la tabla"""
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = f"ALTER TABLE {table_name} RENAME to {new_table_name};"
        cursor.execute(sentence)
        conn.commit()
    except Exception as e:
        print(f"Uy, parece que ha ocurrido un error, investiga: {e}")
    else:
        print(f"Se ha renombrado exitosamente de {table_name} a {new_table_name}")
    finally:
        conn.close()


def delete_table(db_name, table_name):
    """En caso de querer eliminar la tabla simplemente escribe el DB donde se encuentra la tabla y despues escribe el nombre de la tabla a eliminar"""
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = f"DROP TABLE {table_name};"
        cursor.execute(sentence)
        conn.commit()
    except Exception:
        print("Uy, parece que ha ocurrido un error, investiga")
    else:
        print(f"La tabla: {table_name} se ha eliminado con exito")
    finally:
        conn.close()


# rows and more rows, CRUD


def count(db_name, table_name):
    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = (
            """SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = '%s';"""
            % table_name
        )

        cursor.execute(sentence)
        conn.commit()
    except Exception as e:
        print(f"Uy, parece que ha ocurrido un error, investiga: {e}")
    else:
        print("Se han introducido los datos con exito")
    finally:
        conn.close()


def insert(db_name, table_name, row=[]):
    date = datetime.now()

    db = f"{PATH}{db_name}.db"
    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        sentence = f"INSERT INTO {table_name} VALUES({date.date()} "
        for i in row:
            sentence += f"Serie{i+1} TEXT, "

        sentence = sentence.rstrip(", ")
        sentence += ");"
        cursor.execute(sentence)
        conn.commit()
    except Exception as e:
        print(f"Uy, parece que ha ocurrido un error, investiga: {e}")
    else:
        print("Se han introducido los datos con exito")
    finally:
        conn.close()


if __name__ == "__main__":
    # Data Bases CRUD
    # create_db("Test1")
    # list_db()
    # rename_db("Test1", "NewTest")
    # delete_db("NewTest")

    # Tables and more tables
    # create_table("Test2", "Prueba1", 4)
    read_table("Test2", "Prueba1")
    # rename_table("Test2", "Table1", "Table2")
    # delete_table("Test2", "Table2")
    # list_tables("Test2")
