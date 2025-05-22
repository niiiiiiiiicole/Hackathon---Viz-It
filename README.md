# Hackathon---Viz-It
Repo for hackathon project between Nicole, Kelly and Yezi
# Hackathon---Viz-It

## Project Overview

**Hackathon---Viz-It** is designed to empower analytics teams with instant, intuitive visualization capabilities for SQL query outputs. By addressing the challenges of slow exploratory analysis, delayed anomaly detection, and inefficient collaboration, Viz-It streamlines the path from raw data to actionable insights.

## Problem Statement

Analytics teams often struggle to validate data, investigate anomalies, and pinpoint root causes due to the lack of an instant way to visualize SQL query output. Without a quick and intuitive visualization tool, teams face:

- Slower exploratory analysis and data discovery
- Delays in identifying data integrity issues like missing values or anomalies
- Difficulty spotting correlations and comparing trends
- Inefficient root-cause analysis
- Barriers to insight-sharing among analysts working on related domains
- Slower iteration with stakeholders on dashboard design and data presentation

## Solution

**Viz-It** provides a seamless platform for instantly visualizing SQL query results, enabling:

- Faster and more effective exploratory analysis
- Rapid identification of data integrity issues
- Easy detection of trends, correlations, and anomalies
- Streamlined collaboration and sharing of insights
- Accelerated dashboard prototyping and iteration

## Features

- Instant visualization of SQL query outputs
- Multiple chart types for flexible data exploration
- Simple, intuitive user interface designed for analysts
- Easy sharing and collaboration tools

## Before Getting Started

- Before you run the script, you'll want to enable Trino Connections from 1Password.
- Go to https://mongodb.1password.com/
- In the "Employee" vault, add a new "Login" item
- Name this "TrinoCredentials"
- Enter your Trino username and password and save
- Now follow the rest of the instructions on https://developer.1password.com/docs/cli/get-started/#install

## Interacting with Viz It
- Download the Viz It python file (or check it out from GitHub) and open it in Pycharm. Note that Jupyter notebook is not supported with VizIt.
- You will likely need to download the relevant libraries from the import list at the top
- When the script runs successfully, you will see Process finished with exit code 0
- Scroll up where it says "Warning: to view this Streamlit app on a browser, run it with the following
  command: streamlit run [your path name will be here].py"
- Copy paste the "streamlit run ___" command into your Pycharm terminal
- This should launch streamlit into your web browser
- Copy paste your query and viz it!

## Current Limitations
- Your query dataset must be less than 200 MB (you will see an error otherwise)
---

Enjoy!
