# BDM_Project
The project is about a basic analysis of the efficiency of the NYC 311 services.

The main analysis will based on NYC 311 request, an brief additional analysis on LA 311 request is used as a comparison.

# Group Member:
* Yihao Song. [songyihao1992@gmail.com](songyihao1992@gmail.com)
* Derrick Chong. [derrickc0527@gmail.com](derrickc0527@gmail.com)

# Methodology:
## Running on the Hadoop with Sparks
* Make the python file executable

Command:
$ hadoop fs -chmod +x <your python file location>

* Executing files on the cluster. For this project, we used the NYU CUSP

Command:
$ spark-submit --name <name of job> --num-executors <number> <python code location>

# Final Report:
This is the [link](https://github.com/yihaosong/BDM_Project/blob/master/BDM_report2.docx) to our final report.

# About this repository
* Codes used on Hadoop Cluster:
1. [Cluster](https://github.com/yihaosong/BDM_Project/blob/master/la_data.py)
2. [Cluster](https://github.com/yihaosong/BDM_Project/blob/master/nyc_311.py)

* Codes used on Spark Analysis:
1. [Spark](https://github.com/yihaosong/BDM_Project/blob/master/project_test.ipynb)
2. [Spark](https://github.com/yihaosong/BDM_Project/blob/master/311NYC.ipynb)
3. [Spark](https://github.com/yihaosong/BDM_Project/blob/master/nyc_data_view.ipynb)
4. [Spark](https://github.com/yihaosong/BDM_Project/blob/master/la_data_view.ipynb)
