#RealTime_SentimentAnalysis
 
Use Case: Canadian Elections 2025 – Public Sentiment Tracker

This project implements a secure, serverless, real-time data pipeline to ingest tweets related to Canadian political parties and analyze public sentiment using NLP. Results are visualized in Power BI for real-time monitoring and trend analysis.

Tech Stack

- **Twitter API v2** (Filtered Stream API)
- **Azure Event Hub**
- **Azure Blob Storage (ABFS)**
- **Azure Databricks (PySpark + Delta Lake)**
- **Azure Cognitive Services (Text Analytics for Sentiment)**
- **Unity Catalog + Delta Table**
- **Power BI (Live Dashboard)**

Architecture
![README md](https://github.com/user-attachments/assets/a61f41fc-b359-4502-88ba-3563f106d0a4)
Features

- Real-time tweet ingestion using Azure Functions and Twitter API
- Sentiment analysis using Azure AI with party-based classification (Liberal vs Conservative)
- Delta Lake storage with Unity Catalog governance
- OAuth-secured ABFS access using Access Connector
- Live dashboarding in Power BI via Databricks SQL Warehouse


Folder Structure

- `/notebooks`: PySpark Databricks notebook for sentiment processing
- `/azure-function`: Azure Function to fetch tweets and push to Event Hub
- `/powerbi`: Power BI report or screenshot
- `/architecture`: Visual architecture of the full pipeline

Security & Best Practices

- Uses **OAuth2 Access Connector** for ABFS access (no keys in code)
- Fully compliant with Unity Catalog + Databricks governance
- All credentials stored via Azure Key Vault / environment variables

Learning Outcomes

- End-to-end serverless architecture design on Azure
- Hands-on with OAuth, Unity Catalog, ABFS, and Delta Lake
- How to create real-time NLP dashboards using Databricks + Power BI

Contact

Created by Tejasaw Verma

