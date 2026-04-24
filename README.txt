CSE 6242 Polymarket Trader Analysis

DESCRIPTION:

The Polymarket Trader Analysis package is a comprehensive toolkit designed to analyze and visualize trader behavior on decentralized prediction markets. The system processes high-volume raw transaction data to identify patterns in user activity across diverse sectors such as Crypto, Politics, and Sports.

The package utilizes a multi-stage pipeline: data ingestion and transformation via DuckDB, k-means clustering to segment trader profiles based on volume and frequency, and an interactive D3.js visualization platform. The final visualization allows users to explore these trader clusters and their performance metrics in a web-based environment.

INSTALLATION:

Environment Setup -

- Clone the repo: `git clone https://github.com/AnusreeChittineni/polymarket-trader-analysis`
- cd into the repo: `cd polymarket-trader-analysis`

EXECUTION:

For local hosting -
- Ensure you are in the project root
- Start a server: `python -m http.server`
- Visit the site: http://localhost:8000/viz/

For remote hosting -
- In your github repository settings you can deploy a github pages url through github actions
    - Be sure to move clustered_traders.csv to the viz folder

FROM SCRATCH REPLICATION:

Data Preparation -

- Dowload the raw dataset (data.tar.zst) from https://github.com/Jon-Becker/prediction-market-analysis?tab=readme-ov-file 
- Place the data within a data folder in the repo root and run the following processing pipeline
    - Execute create_subset/sample.ipynb to subset the raw data
    - Execute create_subset/analysis.ipynb to generate trader_stats.csv
    - Run the categorization script
        - python explore_data/append_categories.py
    - Generate the final clusters
        - python clustering.py