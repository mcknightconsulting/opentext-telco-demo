FROM python:latest
COPY include .
RUN pip install -r requirements.txt
CMD /bin/bash -c 'while true; do python ./demo.py 360 120 3; done'
