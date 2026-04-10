# NimbusAI — Customer Churn & Retention Analysis

**Candidate:** Siva
**Role:** Data Analyst Intern — Take-Home Assignment
**Company:** RoaDo
**Date:** April 2026

---

# Project Overview

NimbusAI is a B2B SaaS platform experiencing rising customer churn and increased support load.
This project analyzes customer behavior, engagement, and support activity to identify churn drivers and provide actionable retention strategies.

The analysis combines:

* PostgreSQL (relational data)
* MongoDB (event/activity data)
* Python (data cleaning and statistical analysis)
* Power BI (interactive dashboard)
* Video walkthrough explaining the full workflow

The goal was to deliver **data-driven recommendations** for improving retention and increasing revenue.

---

# Dataset Summary

| Metric          | Value  |
| --------------- | ------ |
| Customers       | 1,204  |
| Subscriptions   | 1,840  |
| Support Tickets | 6,000  |
| MongoDB Events  | 51,485 |

---

# Tech Stack

* PostgreSQL
* MongoDB
* Python
* Pandas
* NumPy
* Power BI
* VS Code

---

# Project Tasks Completed

## Task 1 — SQL Analysis

Performed advanced SQL queries including:

* Joins
* Aggregations
* Window functions
* Time-series analysis
* Duplicate detection

Key outputs:

* Active customers per plan
* Average monthly revenue
* Ticket rate trends
* Churn spike detection
* Duplicate account identification

---

## Task 2 — MongoDB Analysis

Built aggregation pipelines to analyze user behavior and engagement.

Key analyses:

* Weekly sessions per user
* Feature usage and retention
* Onboarding funnel drop-off
* Free-tier upsell targeting

---

## Task 3 — Python Data Wrangling & Statistical Analysis

Performed full data processing using Python.

Key steps:

Data extraction from:

* PostgreSQL
* MongoDB

Data cleaning included:

* Fixing invalid dates
* Removing duplicates
* Handling missing values
* Normalizing inconsistent fields

Statistical analysis included:

* Hypothesis testing
* Churn rate comparison
* Customer segmentation

Output:

* Cleaned dataset for dashboard
* Customer segments
* Engagement scores

---

## Task 4 — Power BI Dashboard

Built an interactive dashboard with:

KPIs:

* Total Customers
* Churn Rate
* Average Revenue
* Customer Segments

Visualizations:

* Churn rate by plan
* Churn by industry
* Engagement vs revenue
* Ticket volume vs churn
* Customer segmentation distribution

---

## Task 5 — Video Walkthrough

A recorded video explaining:

* Data setup
* Query logic
* Data cleaning
* Dashboard insights
* Business recommendations

---

# Key Findings

## 1. Free Plan Has Highest Churn

Free users churn significantly more than paid users.

Reason:

Low commitment and limited feature usage.

---

## 2. Support Tickets Predict Churn Risk

Customers with frequent support issues are more likely to churn.

---

## 3. Silent Churn Exists

Some customers leave without submitting support tickets.

This indicates:

Support metrics alone cannot detect churn risk.

---

## 4. Upsell Opportunity Identified

High-engagement free users are strong candidates for paid plans.

---

# Business Recommendations

## Recommendation 1 — Proactive Support Alerts

Trigger customer success outreach after multiple support tickets.

---

## Recommendation 2 — Improve Onboarding

Add guided setup during first login to reduce early drop-off.

---

## Recommendation 3 — Targeted Free-Tier Upsell

Offer trial upgrades to highly engaged free users.

---

# How to Run the Project

## PostgreSQL Setup

Run:

psql -U postgres -d nimbus -f "nimbus_core.sql"

---

## MongoDB Setup

Run:

mongosh nimbus_events --file "C:\Users\Siva\Downloads\nimbus_mongo_queries.js"

---

## Python Script

Run:

python task3_final.py

---

## Power BI Dashboard

Open:

NimbusAI_Dashboard.pbix

---

# Project Structure

project/

task1_sql_queries.sql
nimbus_mongo_queries.js
task3_final.py
clean_for_dashboard.py
nimbus_dashboard_clean.csv
NimbusAI_Dashboard.pbix
README.md

---

# Deliverables

* SQL queries
* MongoDB pipelines
* Python analysis scripts
* Cleaned datasets
* Power BI dashboard
* Video walkthrough

---

# Conclusion

This project demonstrates the full data analytics workflow:

Data extraction
Data cleaning
Statistical analysis
Visualization
Business recommendations

The results provide actionable insights to reduce churn and improve customer retention.

---

# Author

Siva
B.Tech — Information Technology
Data Analytics / Data Science
