# Databricks notebook source
# Install Azure AI Text Analytics SDK
%pip install azure-ai-textanalytics

# COMMAND ----------

# ------------------ Imports ------------------
from pyspark.sql import SparkSession
import pandas as pd
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

# COMMAND ----------

# ------------------ Storage Config ------------------
storage_account_name = "twdrprdcacentral"
container_name = "twitterdatabase"

# COMMAND ----------

# ------------------ Read All Tweets ------------------
path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/*/*/*/"
df = spark.read.parquet(path)
pandas_df = df.toPandas()

# COMMAND ----------

# ------------------ Azure Language Config ------------------
endpoint = ""
key = ""
credential = AzureKeyCredential(key)
client = TextAnalyticsClient(endpoint=endpoint, credential=credential)

# COMMAND ----------

# ------------------ Liberal and Conservative Keywords ------------------
liberal_keywords = [
    "#CanadaElectionS2025", "liberal party", "liberals", "lpc", "justin trudeau", "trudeau", "mark carney", "carney", "#teamtrudeau", "#trudeaumustgo"
]
conservative_keywords = [
    "conservative party", "conservatives", "cpc", "pierre poilievre", "poilievre", "#pierrepoilievre"
]

# COMMAND ----------

# ------------------ Analyze Sentiment with Opinion Mining and Fallback ------------------
def analyze_party_sentiment(documents):
    results = []
    batch_size = 10
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        response = client.analyze_sentiment(documents=batch, show_opinion_mining=True)
        
        for j, doc in enumerate(response):
            liberal_sentiment = None
            conservative_sentiment = None
            liberal_found = False
            conservative_found = False

            if not doc.is_error:
                # Try mined opinions first
                for sentence in doc.sentences:
                    for opinion in sentence.mined_opinions:
                        target = opinion.target.text.lower()
                        sentiment = opinion.target.sentiment
                        print(sentence)
                        #print(f"Target: {target}, Sentiment: {sentiment}")
                        
                        if any(keyword in target for keyword in liberal_keywords):
                            liberal_sentiment = sentiment
                            liberal_found = True
                        if any(keyword in target for keyword in conservative_keywords):
                            conservative_sentiment = sentiment
                            conservative_found = True

                # Fallback to overall document sentiment if necessary
                original_text = batch[j]['text'].lower()

                if not liberal_found:
                    if any(keyword in original_text for keyword in liberal_keywords):
                        liberal_sentiment = doc.sentiment

                if not conservative_found:
                    if any(keyword in original_text for keyword in conservative_keywords):
                        conservative_sentiment = doc.sentiment

                results.append({
                    "id": doc.id,
                    "liberal_sentiment": liberal_sentiment,
                    "conservative_sentiment": conservative_sentiment
                })
            else:
                results.append({
                    "id": doc.id,
                    "liberal_sentiment": "error",
                    "conservative_sentiment": "error"
                })
    return results

# COMMAND ----------

# ------------------ Prepare Documents ------------------
texts = [{"id": str(i), "text": text} for i, text in enumerate(pandas_df['text'].tolist())]

# COMMAND ----------

# ------------------ Run Sentiment Analysis ------------------
party_sentiment_results = analyze_party_sentiment(texts)

# COMMAND ----------

# ------------------ Merge with Original Tweets ------------------
party_sentiment_df = pd.DataFrame(party_sentiment_results)
pandas_df = pandas_df.reset_index()
pandas_df['id'] = pandas_df['index'].astype(str)
party_sentiment_df['id'] = party_sentiment_df['id'].astype(str)
final_df = pd.merge(pandas_df, party_sentiment_df, on="id", how="left")

# COMMAND ----------

# ------------------ Final Refinement ------------------
# Only keep author_id, created_at, liberal_sentiment, conservative_sentiment
# Convert created_at to datetime
# Sort by author_id and latest created_at
# Keep latest tweet per author
# Reset index for clean output
refined_df = final_df[['author_id', 'created_at', 'text', 'liberal_sentiment', 'conservative_sentiment']]
refined_df['created_at'] = pd.to_datetime(refined_df['created_at'])
refined_df = refined_df.sort_values(by=['author_id', 'created_at'], ascending=[True, False])
refined_df = refined_df.drop_duplicates(subset=['author_id'], keep='first')
refined_df = refined_df.reset_index(drop=True)

# COMMAND ----------

# New (abfss://, delta format)
output_path = f"abfss://sentimentresults@{storage_account_name}.dfs.core.windows.net/sentiment_results_refined/"
refined_spark_df = spark.createDataFrame(refined_df)
refined_spark_df.write.format("delta").mode("overwrite").save(output_path)
print("Refined Sentiment Results saved as Delta Table")

# COMMAND ----------

%sql
CREATE DATABASE IF NOT EXISTS sentiment_db
COMMENT "Database for Canadian Twitter Sentiment Analysis";
USE sentiment_db;
DROP TABLE IF EXISTS sentiment_results;
CREATE TABLE IF NOT EXISTS sentiment_results
USING DELTA
LOCATION 'abfss://sentimentresults@twdrprdcacentral.dfs.core.windows.net/sentiment_results_refined/';