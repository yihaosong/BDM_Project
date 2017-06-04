# BDM_Project
The project is about a basic analysis of the efficiency of the NYC 311 services.

The main analysis will based on NYC 311 request, an brief additional analysis on LA 311 request is used as a comparison.

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

