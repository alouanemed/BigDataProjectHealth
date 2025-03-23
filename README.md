<div align="center">
  <img src="https://raw.githubusercontent.com/alouanemed/regional-anomaly-mapreduce/assets/morocco_health_banner.png" alt="Moroccan Health Data Anomaly Detection" width="600">
  <h1><samp>🔍 Regional Anomaly Detection in Moroccan Public Health Data using MapReduce</samp></h1>
  <p>
    <samp> Detecete patterns and anomalies within the Moroccan healthcare landscape with Hadoop MapReduce</samp>
  </p>
</div>

<br>

<div align="center">
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
  <img src="https://img.shields.io/badge/Language-Python-blue.svg" alt="Language: Python">
  <img src="https://img.shields.io/badge/Framework-MapReduce-orange.svg" alt="Framework: MapReduce">
  <img src="https://img.shields.io/badge/Data-HDFS-brightgreen.svg" alt="Data: HDFS">
</div>

<br>

## <samp>📜 Project Overview</samp>

This project report details the development and execution of a Big Data pipeline designed to detect regional anomalies within public health data in Morocco. Leveraging the power of the Hadoop ecosystem and the MapReduce framework, this initiative aims to uncover unusual patterns and behaviors in healthcare data across different regions of Morocco. The insights gained from this analysis can contribute to improved healthcare quality, better resource allocation, and proactive identification of potential health crises.

This project was undertaken as part of the **Licence d’excellence, Systèmes d’Information et Intelligence Artificielle (SIIA), M01 : Écosystèmes Big Data** program.

## <samp>✨ Key Features</samp>

* **Regional Anomaly Detection:** Identifies deviations from expected healthcare patterns at the regional level within Morocco.
* **MapReduce Implementation:** Utilizes the distributed computing capabilities of MapReduce for efficient processing of large datasets.
* **Data Enrichment:** Employs Python scripting to enrich a Kaggle healthcare dataset with realistic Moroccan regional data based on population distribution.
* **Three-Phase Processing Pipeline:** Implements a structured approach with distinct phases for baseline computation, anomaly labeling, and anomaly aggregation.
* **Insightful Visualizations:** Generates visualizations to illustrate the distribution of billed amounts and the number of detected anomalies per region.
* **Focus on Public Health:** Addresses the critical context of healthcare challenges and the potential of Big Data in Morocco.

## <samp>🗺️ Table of Contents</samp>

* [📜 Project Overview](#-project-overview)
* [✨ Key Features](#-key-features)
* [🗺️ Table of Contents](#️-table-of-contents)
* [🚀 Getting Started](#-getting-started)
    * [Prerequisites](#prerequisites)
    * [Setup](#setup)
* [💾 Data](#-data)
    * [Data Sources](#data-sources)
    * [Data Enrichment](#data-enrichment)
* [🛠️ Tools and Technologies](#️-tools-and-technologies)
* [⚙️ Project Architecture](#️-project-architecture)
* [💡 Processing Approach](#-processing-approach)
    * [Phase 1: Baseline Computation](#phase-1-baseline-computation)
    * [Phase 2: Anomaly Labeling](#phase-2-anomaly-labeling)
    * [Phase 3: Anomaly Aggregation](#phase-3-anomaly-aggregation)
* [📊 Results](#-results)
    * [Visualizations](#visualizations)
* [🎯 Potential Implications](#-potential-implications)
* [🚧 Limitations](#-limitations)
* [🌱 Future Improvements](#-future-improvements)
* [📄 License](#-license)
* [🙏 Acknowledgements](#-acknowledgements)

## <samp>🚀 Getting Started</samp>

Get ready to dive into the world of Big Data-driven anomaly detection! Follow these steps to understand and potentially reproduce the project.

### <samp>Prerequisites</samp>

Ensure you have the following installed and configured:

* **Hadoop Environment:** A running Hadoop cluster with HDFS access is essential for executing the MapReduce jobs.
* **Python 3.x:** Required for data preprocessing, enrichment, and potentially for running auxiliary scripts.
* **Bash:** Used for scripting and automating the execution of the pipeline.

### <samp>Setup</samp>

1.  **Clone the Repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```
2.  **Data Ingestion:** The enriched dataset (as described in the report) should be uploaded to HDFS. Refer to section **1.6.8 Ingestion dans HDFS** of the project report for the specific HDFS paths.
3.  **MapReduce Jobs:** The MapReduce jobs (for each phase of the pipeline) need to be compiled and available in your Hadoop environment.
4.  **Execution Scripts:** The Bash scripts used to orchestrate the pipeline execution (as mentioned in section **1.7.3 Bash**) should be reviewed and adapted to your specific Hadoop setup.

## <samp>💾 Data</samp>

This project utilizes a healthcare dataset enriched with Moroccan regional information.

### <samp>Data Sources</samp>

The initial dataset was sourced from:

* **Kaggle Healthcare Dataset:** A synthetic dataset providing a foundation for patient demographics, medical conditions, and billing information. ([https://www.kaggle.com/datasets/prasad22/healthcare-dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset))

### <samp>Data Enrichment</samp>

To simulate realistic Moroccan public health data, the Kaggle dataset was enhanced using Python scripts to:

* **Randomly map hospitals to the 12 administrative regions of Morocco** based on their population distribution.
* **Add a 'Region' column** to each record, indicating the assigned Moroccan region.
* **Include a 'Population Régionale' column** representing the population of the assigned region, used for normalization in later analysis.

## <samp>🛠️ Tools and Technologies</samp>

The core technologies employed in this project include:

* **MapReduce:** The primary framework for distributed processing of the healthcare data on the Hadoop cluster.
* **Python:** Used for data preprocessing, regional mapping, and enrichment of the initial dataset. Libraries like Pandas and NumPy were likely utilized.
* **Bash:** Employed for scripting and automating the execution of the data pipeline, including data loading, running Python scripts, and triggering MapReduce jobs.
* **Hadoop Distributed File System (HDFS):** The distributed storage system used to store and manage the large healthcare dataset.

## <samp>⚙️ Project Architecture</samp>

The project follows a well-defined Big Data pipeline architecture, as illustrated in the project report (Figure 2). The key stages involve:

1.  **Data Ingestion:** Loading the enriched CSV data into HDFS.
2.  **Phase 1: Baseline Computation (MapReduce):** Calculating the average and standard deviation of billing amounts for each region and month.
3.  **Phase 2: Anomaly Labeling (MapReduce):** Utilizing the baseline statistics to calculate a Z-score for each billing transaction and label it as an anomaly if it exceeds a predefined threshold.
4.  **Phase 3: Anomaly Aggregation (MapReduce):** Aggregating the detected anomalies by region to provide an overview of regional anomalies.
5.  **Visualization:** Presenting the results through charts and graphs, showcasing the distribution of billed amounts and the count of anomalies per region.

## <samp>💡 Processing Approach</samp>

The data processing was strategically divided into three distinct phases:

### <samp>Phase 1: Baseline Computation</samp>

* **Objective:** Establish a baseline for billing amounts for each region on a monthly basis.
* **Method:** A MapReduce job extracts the year and month from the admission date and combines it with the region to form a key. It then aggregates the billing amounts to calculate the sum, sum of squares, and count for each (region, year-month) combination.
* **Output:** A CSV file in HDFS containing the average and standard deviation of billing amounts for each region and month.

### <samp>Phase 2: Anomaly Labeling</samp>

* **Objective:** Identify individual billing transactions that deviate significantly from the established baseline.
* **Method:** A MapReduce job loads the baseline statistics into a distributed cache or memory. For each billing transaction, it calculates a Z-score based on the transaction amount, the mean, and the standard deviation for the corresponding region and month. Transactions with an absolute Z-score above a defined threshold (e.g., 2.0) are labeled as anomalies.
* **Output:** A CSV file in HDFS containing each transaction along with its Z-score and an 'isAnomaly' flag.

### <samp>Phase 3: Anomaly Aggregation</samp>

* **Objective:** Summarize the number of anomalies detected in each region.
* **Method:** A MapReduce job counts the number of transactions flagged as anomalies for each region.
* **Output:** A CSV file in HDFS providing the total count of anomalies per region.

## <samp>📊 Results</samp>

The project successfully generated insightful results, including:

### <samp>Visualizations</samp>

* **Distribution of Billed Amounts:** Visual representations showing how the billed amounts are distributed across different regions and years.
* **Number of Anomalies Detected:** Charts illustrating the count of anomalies identified in each region, providing a clear overview of potential areas of concern.

These visualizations (as seen in the project report) help in understanding the patterns of healthcare spending and highlighting regions with unusual activity.

## <samp>🎯 Potential Implications</samp>

The findings of this project have several potential implications for public health in Morocco:

* **Early Detection of Issues:** Identifying regional anomalies could signal potential issues such as fraudulent billing, unusual disease outbreaks, or disparities in healthcare service utilization.
* **Improved Resource Allocation:** Understanding regional variations can inform better allocation of healthcare resources and infrastructure.
* **Enhanced Public Health Monitoring:** The anomaly detection pipeline can serve as a valuable tool for continuous monitoring of public health trends.

## <samp>🚧 Limitations</samp>

While this project provides valuable insights, it's important to acknowledge its limitations:

* **Synthetic Data Enrichment:** The regional data was generated through random mapping based on population, which might not perfectly reflect real-world healthcare dynamics.
* **Threshold-Based Anomaly Detection:** The anomaly detection relies on a fixed Z-score threshold, which might need to be adjusted based on further analysis and domain expertise.
* **Limited Toolset:** The project was constrained to using MapReduce, Python, and Bash. Utilizing other tools from the Hadoop ecosystem (like Spark or Hive) could potentially offer more advanced analysis and performance improvements.

## <samp>🌱 Future Improvements</samp>

To further enhance this project, the following improvements could be considered:

* **Integration of Real-World Data:** Incorporating actual public health data from Moroccan healthcare institutions would significantly increase the accuracy and relevance of the analysis.
* **Advanced Anomaly Detection Techniques:** Exploring more sophisticated anomaly detection algorithms beyond the Z-score method could yield more nuanced results.
* **Real-time Data Processing:** Implementing a real-time or near real-time data processing pipeline using frameworks like Apache Spark Streaming could enable timely detection of emerging anomalies.
* **Interactive Dashboards:** Developing interactive dashboards using tools like Tableau or Power BI would provide a more user-friendly way to explore the results.
* **Incorporating External Factors:** Integrating external factors like socio-economic indicators or environmental data could provide a more comprehensive understanding of the drivers behind regional health anomalies.

## <samp>📄 License</samp>

This project is licensed under the **Licence d’excellence**.

## <samp>🙏 Acknowledgements</samp>

This project was conducted as part of the USMS Morocco Écosystèmes Big Data program under the guidance of **Prof. Khourdrifi**.
