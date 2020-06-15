FROM python:3.7
WORKDIR /usr/src/app
#RUN git clone https://github.com/lvthillo/docker-ghost-mysql.git .
#RUN git clone https://github.com/cmdkid/elasticsearchParser.git
RUN git clone -b dev https://github.com/cmdkid/elasticsearchParser.git . && git fetch origin &&  git pull
RUN pip install -r requirements.txt
#CMD ["python", "run.py"]
