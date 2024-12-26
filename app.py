from fastapi import FastAPI, Query, Path, Header, HTTPException
from datetime import datetime
import aiomysql

app = FastAPI()


async def get_db_pool():
    return await aiomysql.create_pool(
        host='localhost',
        port=3306,
        user='root',
        password='password',
        db='project_db',
        minsize=5,
        maxsize=10
    )


async def create_database():
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("CREATE DATABASE IF NOT EXISTS project_db;")
        await connection.commit()
    pool.close()
    await pool.wait_closed()


async def create_table():
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                timestamp TIMESTAMP,
                x_client_version VARCHAR(255),
                content TEXT
            );
            """)
        await connection.commit()
    pool.close()
    await pool.wait_closed()


@app.on_event('startup')
async def startup_event():
    await create_database()
    await create_table()


@app.get('/data')
async def get_data(
    user_id_get: int = Path(..., description='Client id'),
    timestamp_get: str = Query(None, description='Client time'),
    x_client_version_get: str = Header(..., description='Client version')
):

    if user_id_get is None or x_client_version_get is None:
        raise HTTPException(status_code=400, detail='Please enter valid data')

    if timestamp_get is None:
        timestamp_get = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    user_id = f'Hello {user_id_get}'

    pool = await get_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                """INSERT INTO data (user_id, timestamp, x_client_version)
                VALUES (%s, %s, %s)""", (user_id_get, timestamp_get, x_client_version_get)
            )
        await connection.commit()
    pool.close()
    await pool.wait_closed()

    response = {
        'user_id': user_id,
        'timestamp': timestamp_get,
        'X_Client_Version': x_client_version_get
    }

    return {'result': response}
