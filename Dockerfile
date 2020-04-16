FROM python:3.7
WORKDIR /usr/src/app
RUN git clone 
RUN pip install -r requirements.txt
RUN python main.py
