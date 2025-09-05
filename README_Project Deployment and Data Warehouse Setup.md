# Data Engineering Project №5:
# Project Deployment and Data Warehouse Setup Mart                |

# Project Overview

This project demonstrates the process of deploying two services (dds_service and cdm_service) 
into a Kubernetes cluster and preparing schemas for staging (STG), data warehouse 
(DDS, based on Data Vault), and customer data mart (CDM).

The work was done step by step, with a focus on making the services run in Yandex Cloud 
and validating that data is loaded correctly into the database tables.

## Deployment Steps

1. Environment Preparation
    Configured yc, kubectl, and helm to work with the practicum Kubernetes cluster.
    Connected Docker to Yandex Container Registry for image storage.

2. Kafka Setup
    Registered Kafka connection with a provided host, topic, and credentials.
    Used curl endpoints to reset and re-register Kafka settings.

3. Redis Data Loading
    Uploaded users and restaurants data into Redis using provided API endpoints.

4. Building and Deploying Services
    Built Docker images for both services:
        dds_service
        cdm_service
    Pushed them into Yandex Container Registry.
    Deployed both services into the cluster using Helm with custom image tags.
    Verified deployments and checked logs in Kubernetes.

### Database Schemas

The database was structured into three main layers:

1. STG (staging)
    Created schema stg.order_events for raw event data coming from Kafka.

2. DDS (data warehouse, Data Vault model)
    Created hubs (h_user, h_category, h_product, h_restaurant, h_order).
    Created links (l_order_product, l_product_restaurant, l_product_category, l_order_user).
    Created satellites for attributes and history tracking (s_user_names, s_product_names, s_order_cost, s_order_status, s_restaurant_names).

3. CDM (customer data mart)
    Created aggregated tables for analytics:
        user_product_counters
        user_category_counters
    These tables support queries on how many times a user ordered a specific product or from a product category.

###  Validation
    Kafka: Confirmed that messages were produced and consumed correctly.
    PostgreSQL: Verified that records were inserted into the subscribers_feedback table and that the DDS/CDM schemas were created successfully.
    Kubernetes: Checked service pods (dds-service and cdm-service) — both were running without errors.

### Result: This project was mainly focused on deploying services, preparing the database structure, and verifying end-to-end data flow from Kafka to the warehouse and marts.
