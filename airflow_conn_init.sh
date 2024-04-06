#!/bin/bash

# init airflow connections


airflow connections add --conn-type http --conn-host https://fcsapi.com/api-v3/forex/latest?symbol=USD/TWD,JPY/TWD&access_key=YOUR_KEY forex_api

airflow connections add --conn-type postgres --conn-host postgres --conn-login airflow --conn-password airflow --conn-port 5432 postgres
