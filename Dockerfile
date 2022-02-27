FROM python:3.9.10
COPY . /home
RUN pip install -r requirements.txt

WORKDIR /home

CMD ["/usr/local/bin/python", "-m", "main"]
