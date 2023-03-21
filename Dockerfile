FROM python:3.9
WORKDIR /opt/kentik/kentik-oci/
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
#RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
#RUN chmod +x /usr/local/bin/dumb-init
COPY kentik_oci_flow.py kentik_oci_flow.py
CMD ["python3","-u","kentik_oci_flow.py"]
