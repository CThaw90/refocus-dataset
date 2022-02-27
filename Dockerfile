FROM python:3.9.10
RUN pip install -r requirements.txt
COPY . /home

WORKDIR /home

CMD ["/usr/local/bin/python", "-m", "main"]
