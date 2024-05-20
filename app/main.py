from fastapi import FastAPI, UploadFile, File
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer

app = FastAPI()

# URL de la base de datos MySQL 
DATABASE_URL = "mysql+pymysql://<usuario>:<contraseña>@<hosting>/data_analytics"

# Crear motor de conexión a la base de datos
engine = create_engine(DATABASE_URL)

# Definir la metadata y la tabla
metadata = MetaData()

csv_data = Table(
    'csv_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('column1', Integer),
    Column('column2', Integer)
)

# Crear la tabla en la base de datos si no existe
metadata.create_all(engine)

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    """
    Endpoint para subir archivos CSV y posteriormente guardar en la base de datos.
    """
    # Esta linea es para leer archivo CSV
    df = pd.read_csv(file.file)

    # Para contar las filas y columnas del CSV
    row_count, column_count = df.shape

    # Insertar datos en la base de datos
    with engine.connect() as connection:
        for index, row in df.iterrows():
            insert_statement = csv_data.insert().values(column1=row['column1'], column2=row['column2'])
            connection.execute(insert_statement)

    # Devuelve el valor de las filas y columnas
    return {"row_count": row_count, "column_count": column_count}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
