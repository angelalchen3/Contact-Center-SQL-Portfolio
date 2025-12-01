# SQL Portfolio – Contact Center & Dialer Analytics

## Purpose

This repository is meant to:
- Demonstrate SQL proficiency for data engineering and analytics roles  
- Provide examples of production-level logic without exposing sensitive or proprietary information  
- Show familiarity with contact center and outbound-dialer data models  

---

## Background

This repository contains anonymized SQL examples based on real analytics and data engineering work.  
The queries demonstrate skills in data modeling, time-series analysis, compliance logic, IVR analytics, 
dialer performance measurement, and Snowflake SQL optimization.

All scripts are fully redacted and use synthetic or generic naming conventions.  
The focus is on the underlying logic, not proprietary data.

---

## Contents

### **1. Agent Interaction Metrics**
Examples of aggregating inbound agent interactions:
- Daily and interval-based metrics
- Time normalization
- Talk, hold, ACW, and transfer logic
- Location standardization

### **2. Queue and Service Level Analytics**
Scripts that calculate:
- Service levels by hour
- Answered vs. abandoned interactions
- Quick abandons
- Flow-out behavior

### **3. IVR Journey Analysis**
Logic for summarizing IVR interactions:
- Containment vs. transfer behavior
- IVR duration calculations
- 30-minute interval bucketing

### **4. Outbound Dialer Performance**
Anonymized outbound-dialer models demonstrating:
- Dial counts vs. account counts
- Connection and payment analysis
- Talk/hold/wrap behavior
- Relationship to dialer load files

### **5. Utility CTE Patterns**
The repo includes reusable SQL structures:
- Parameter tables
- Exclusion lists
- Time bucketing
- Clean dimension joins

---

## Technology

All queries are written using **Snowflake SQL** with:
- CTEs (`WITH` clauses)
- Window functions
- Conditional logic
- Date/time manipulation
- Semi-structured table joins

Queries follow warehouse-style naming (e.g., `fact_`, `dim_`) to reflect analytics engineering patterns.

---

## Notes

- All schemas, tables, columns, queue names, and values have been anonymized.
- No operational rules, thresholds, or internal data structures are included.
- These examples are strictly for showcasing SQL logic and modeling techniques.

