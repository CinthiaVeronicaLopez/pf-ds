@REM POSTGRESSQL LOCAL
@REM net stop postgresql-x64-16
@REM net pause postgresql-x64-16
@REM net start postgresql-x64-16

@REM GUARDAR DEPENDENCIAS DE PYTHON
@REM pip freeze > requirements.txt 

@REM INSTALAR DEPENDENCIAS DE PYTHON
@REM pip install --no-cache-dir -r requirements.txt

@REM LEVANTAR PROYECTO EN DOCKER
@REM docker build -t pf .
@REM docker run -p 80:80 pf
docker compose up
