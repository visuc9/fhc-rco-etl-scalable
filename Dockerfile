# set base image (host OS)
FROM python:3.7

# set the working directory in the container
WORKDIR /RCO

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && apt-get install -y gcc unixodbc-dev
# install OS dependencies
#RUN apt-get install -y python3-psycopg2


# Add SQL Server ODBC Driver 17 for Ubuntu 18.04
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
 
RUN apt-get install unixodbc-dev
 

# copy the dependencies file to the working directory
COPY requirements.txt .
COPY encryptsecret.py .
# install python dependencies
RUN pip install -r requirements.txt

# copy the Application Content to the working directory
COPY App/ ./App/
ADD logs/log.txt ./logs/
COPY .env .
# command to run on container start
CMD [ "python", "-m", "App.rco_main" ]