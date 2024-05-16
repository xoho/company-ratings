# Social-Credit Ratings

Social-Credit Ratings manages the social credit ratings for a 
group of users and public companies. Users can view their own ratings and
update personal information.

Users can also search for other users. The view of other users only shows the current rating score and
any public information the target user has chosen to share.

This app is written in flask with a sqlalchmey supported backend (e.g., sqlitte, postgres).

## App setup

Generate an .env using:

    cd src && python config.py > .env

and then alter to desired settings. 

## Docker compose

bring up the app with

    docker compose up -d

## DB setup - POSTGRES

    CREATE ROLE svc_socialcredit LOGIN PASSWORD '<password>' CREATEROLE;
    CREATE DATABASE socialcredit;
    GRANT ALL PRIVILEGES ON DATABASE socialcredit TO svc_socialcredit;
    ALTER DATABASE socialcredit OWNER TO svc_socialcredit;
    SET ROLE svc_socialcredit;
    \c socialcredit;
    GRANT ALL PRIVILEGES ON SCHEMA public TO svc_socialcredit WITH GRANT OPTION;

    ALTER DEFAULT PRIVILEGES IN SCHEMA PUBLIC GRANT ALL PRIVILEGES ON TABLES TO svc_socialcredit WITH GRANT OPTION;
    ALTER DEFAULT PRIVILEGES IN SCHEMA PUBLIC GRANT ALL PRIVILEGES ON SEQUENCES TO svc_socialcredit WITH GRANT OPTION;
    ALTER DEFAULT PRIVILEGES IN SCHEMA PUBLIC GRANT ALL PRIVILEGES ON FUNCTIONS TO svc_socialcredit WITH GRANT OPTION;