 _____________________________________________________________________________
|                         Data Engineering Project №4:                        |
|                                                                             |
|                             Streaming pipeline                              |
|_____________________________________________________________________________|

# Project Overview

This project demonstrates working with a using Apache Kafka, Apache Spark Structured 
Streaming, and PostgreSQL.
The goal was to consume campaign data from a Kafka topic, enrich it with restaurant 
subscriber data from PostgreSQL, and then write the processed results back into a 
new Kafka topic and into PostgreSQL.

## Main script: kafka_stream.py

Project Steps
1. Kafka Producer & Consumer Test
    Tested Kafka connection with kafkacat.
    Sent example campaign messages to the input topic (tani-astroblog).
    Verified messages consumption from Kafka.

2. PostgreSQL Setup
Created two tables in PostgreSQL:
    subscribers_restaurants – stores restaurant subscribers.
    subscribers_feedback – stores processed campaign messages.
Indexes were added for faster lookups on client_id and restaurant_id.

3. Spark Streaming Script (kafka_stream.py)
    Read campaign messages from Kafka.
    Joined with PostgreSQL subscribers data.
    Wrote enriched messages to:
        Kafka output topic (tani-astroblog_out)
        PostgreSQL table (subscribers_feedback).

4. Validation
    Verified Kafka output topic messages with Spark consumer.
    Checked PostgreSQL records (subscribers_feedback) — 42 rows successfully written.

## Tech Stack
    Apache Kafka – message broker
    Apache Spark (Structured Streaming) – stream processing
    PostgreSQL – storage for subscribers and feedback
    Docker – containerized environment

##  Example Query Results
Kafka Output Messages
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","client_id":"423e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"}

##  PostgreSQL Feedback Table Sample
id	restaurant_id	adv_campaign_id	client_id	processing_date
1	123e4567…	123e4567…	423e4567…	2025-05-31
2	123e4567…	123e4567…	723e4567…	2025-05-31

### Result: The streaming pipeline was successfully implemented: Kafka → Spark → PostgreSQL + Kafka output.
