FROM python:3.9.10

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD main.py /home
ADD resource /home/resource
ADD common /home/common
ADD data /home/data

WORKDIR /home

CMD ["/usr/local/bin/python", "-m", "main"]
