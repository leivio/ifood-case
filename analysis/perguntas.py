# Databricks notebook source

%sql
-- Pergunta 1: Qual a média de valor total (total\_amount) recebido em um mês considerando todos os yellow táxis da frota?
SELECT pickup_year, pickup_month, ROUND(AVG(total_amount), 2) AS media_total_amount
FROM frt_am.gold.nyc_tlc__yellowtripdata
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month

# COMMAND ----------

%sql
-- Pergunta 2:  Qual a média de passageiros (passenger\_count) por cada hora do dia
-- que pegaram táxi no mês de maio considerando todos os táxis da frota?
SELECT pickup_hour, ROUND(AVG(passenger_count), 2) AS media_passageiros
FROM frt_am.gold.nyc_tlc__yellowtripdata
WHERE pickup_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour

# COMMAND ----------
