-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;padding-top:10px;text-family:Ariel">Report on Dominant F1 drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dominant_drivers_view
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name FROM dominant_drivers_view WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year ASC, avg_points DESC

-- COMMAND ----------

WITH RankedDrivers AS (
  SELECT race_year, 
         driver_name,
         COUNT(*) AS total_races,
         SUM(calculated_points) AS total_points,
         AVG(calculated_points) AS avg_points,
         RANK() OVER(PARTITION BY race_year ORDER BY AVG(calculated_points) DESC) AS rank
  FROM f1_presentation.calculated_race_results
  GROUP BY race_year, driver_name
  HAVING COUNT(*) > 10
)
SELECT race_year, 
       driver_name,
       total_races,
       total_points,
       avg_points
FROM RankedDrivers
WHERE rank = 1
ORDER BY race_year DESC

-- COMMAND ----------


