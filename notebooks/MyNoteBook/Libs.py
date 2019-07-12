# Databricks notebook source
# MAGIC %sh 
# MAGIC pip install --upgrade pip

# COMMAND ----------

# MAGIC %sh
# MAGIC pip list

# COMMAND ----------

dbutils.library.help()

# COMMAND ----------

import pkg_resources
pkg_resources.get_distribution('scipy').version


# COMMAND ----------

dbutils.library.installPyPI('scipy','1.2.0')
dbutils.library.restartPython()
dbutils.library.list()

# COMMAND ----------

import pkg_resources
pkg_resources.get_distribution('scipy').version

# COMMAND ----------

