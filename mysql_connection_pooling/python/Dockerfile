FROM python:3.9

EXPOSE 3306

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


RUN pip install mysqlclient
RUN pip install mysql-connector-python
COPY . .

RUN chmod +x start_script.sh

CMD ./start_script.sh