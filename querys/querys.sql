-- Qual a porcentagem de crimes que usaram armas de fogo ou brancas?
SELECT ROUND(((SELECT COUNT(1) FROM `dw-lab1-dsa.crimes_la.Crimes_La` WHERE weapon_used_id NOT IN (500, 511, 400))/COUNT(1))*100) crimes_with_weapons_percentage
FROM `dw-lab1-dsa.crimes_la.Crimes_La`  

-- Qual é a divisão de vítimas por faixa etária?
SELECT CASE WHEN v.age BETWEEN 0 AND 10 THEN '0-10'
            WHEN v.age BETWEEN 11 AND 20 THEN '10-20'
            WHEN v.age BETWEEN 21 AND 30 THEN '20-30'
            WHEN v.age BETWEEN 31 AND 40 THEN '30-20'
            WHEN v.age BETWEEN 41 AND 50 THEN '40-20'
            WHEN v.age BETWEEN 51 AND 60 THEN '50-20'
            WHEN v.age BETWEEN 61 AND 70 THEN '60-20'
            ELSE '70+'
        END age_range, COUNT(1) count_victims
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
INNER JOIN `dw-lab1-dsa.crimes_la.Victims` v
ON cla.id = v.crime_id
WHERE v.age > 0
GROUP BY age_range
ORDER BY age_range

-- Qual a arma mais comum de ser usada em crimes?
SELECT w.description, w.id, COUNT(1) count_weapon_used
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
INNER JOIN `dw-lab1-dsa.crimes_la.Weapons` w
ON cla.weapon_used_id = w.id
WHERE w.id NOT IN (500)
GROUP BY w.description, w.id
ORDER BY count_weapon_used DESC

-- Qual foi a área com a maior quantidade de crimes de 2020 até março de 2024?
SELECT a.description, COUNT(1) count_crimes
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cl
INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_Date` cd
ON cl.id = cd.crime_id
INNER JOIN `dw-lab1-dsa.crimes_la.Location` l
ON cl.id = l.crime_id
INNER JOIN `dw-lab1-dsa.crimes_la.Areas` a
ON l.area_id = a.id
WHERE cd.datetime_occ BETWEEN '2020-01-01' AND '2024-03-31'
GROUP BY a.description
ORDER BY COUNT(1) DESC;

-- Qual é o perfil de vítma que é mais afetado pelos crimes?
SELECT v.sex, vd.description, COUNT(1) count_crimes
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
INNER JOIN `dw-lab1-dsa.crimes_la.Victims` v
ON cla.id = v.crime_id
INNER JOIN `dw-lab1-dsa.crimes_la.Victim_Descent` vd
ON v.descent_id = vd.id
WHERE v.sex <> 'X'
AND vd.description <> 'Unknown'
GROUP BY v.sex, vd.description
ORDER BY count_crimes DESC

-- Qual foi o mês com a maior quantidade de crimes reportados?
SELECT EXTRACT(YEAR FROM cd.Date_Rpt) year, CASE WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 1 THEN 'January'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 2 THEN 'February'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 3 THEN 'March'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 4 THEN 'April'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 5 THEN 'May'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 6 THEN 'June'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 7 THEN 'July'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 8 THEN 'August'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 9 THEN 'September'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 10 THEN 'October'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 11 THEN 'November'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 12 THEN 'December'
                                                END month, COUNT(1) count_crimes
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_Date` cd 
ON cla.id = cd.crime_id
GROUP BY year, month
ORDER BY count_crimes DESC

-- Quais foram os meses com a maior quantidade de crimes de cada ano? E os menores?
WITH crimes_by_month AS (
SELECT EXTRACT(YEAR FROM cd.Date_Rpt) year, CASE WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 1 THEN 'January'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 2 THEN 'February'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 3 THEN 'March'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 4 THEN 'April'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 5 THEN 'May'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 6 THEN 'June'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 7 THEN 'July'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 8 THEN 'August'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 9 THEN 'September'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 10 THEN 'October'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 11 THEN 'November'
                                                 WHEN EXTRACT(MONTH FROM cd.Date_Rpt) = 12 THEN 'December'
                                                END month, COUNT(1) count_crimes
FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_Date` cd 
ON cla.id = cd.crime_id
GROUP BY year, month
ORDER BY count_crimes DESC
)
SELECT *, RANK() OVER (PARTITION BY year ORDER BY count_crimes DESC) rank_crimes
FROM crimes_by_month
ORDER BY rank_crimes

-- Qual o tipo de crime mais comum? E o menos comum?
WITH count_Crm_Cd AS (
  SELECT Crm_Cd, c.description,COUNT(1) count_crimes
  FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_List` cl
  ON cla.id = cl.crime_id
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes` c
  ON cl.Crm_Cd = c.id
  GROUP BY cl.Crm_cd, c.description
  ORDER BY count_crimes DESC
), count_Crm_Cd_2 AS (
  SELECT Crm_Cd_2, c.description, COUNT(1) count_crimes_2
  FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_List` cl
  ON cla.id = cl.crime_id
  LEFT JOIN `dw-lab1-dsa.crimes_la.Crimes` c
  ON cl.Crm_Cd_2 = c.id
  WHERE c.description IS NOT NULL
  GROUP BY cl.Crm_cd_2, c.description
  ORDER BY count_crimes_2 DESC
), count_Crm_Cd_3 AS (
  SELECT Crm_Cd_3, c.description, COUNT(1) count_crimes_3
  FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_List` cl
  ON cla.id = cl.crime_id
  LEFT JOIN `dw-lab1-dsa.crimes_la.Crimes` c
  ON cl.Crm_Cd_3 = c.id
  WHERE c.description IS NOT NULL
  GROUP BY cl.Crm_cd_3, c.description
  ORDER BY count_crimes_3 DESC
), count_Crm_Cd_4 AS (
  SELECT Crm_Cd_4, c.description, COUNT(1) count_crimes_4
  FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_List` cl
  ON cla.id = cl.crime_id
  LEFT JOIN `dw-lab1-dsa.crimes_la.Crimes` c
  ON cl.Crm_Cd_4 = c.id
  WHERE c.description IS NOT NULL
  GROUP BY cl.Crm_cd_4, c.description
  ORDER BY count_crimes_4 DESC
)
SELECT cd.description, cd.Crm_Cd, COALESCE(count_crimes, 0) + COALESCE(count_crimes_2, 0) + COALESCE(count_crimes_3, 0) + COALESCE(count_crimes_4, 0) sum_crimes
FROM count_Crm_Cd cd
LEFT JOIN count_Crm_Cd_2 cd2
ON cd.Crm_Cd = cd2.Crm_Cd_2
LEFT JOIN count_Crm_Cd_3 cd3
ON cd.Crm_Cd = cd3.Crm_Cd_3
LEFT JOIN count_Crm_Cd_4 cd4
ON cd.Crm_Cd = cd4.Crm_Cd_4
ORDER BY sum_crimes DESC


-- Qual é a média móvel de crimes no mês de novembro de 2021?
WITH Dates AS (
  SELECT CAST(Datetime_OCC AS DATE) Date_OCC, COUNT(1) count_occ
  FROM `dw-lab1-dsa.crimes_la.Crimes_La` cla
  INNER JOIN `dw-lab1-dsa.crimes_la.Crimes_Date` cd 
  ON cla.id = cd.crime_id
  GROUP BY Date_OCC
)
SELECT Date_OCC, ROUND(AVG(count_occ) OVER (ORDER BY Date_OCC ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING), 2) media_movel
FROM Dates
WHERE EXTRACT(YEAR FROM Date_OCC) = 2021
AND EXTRACT(MONTH FROM Date_OCC) = 11
ORDER BY EXTRACT(DAY FROM Date_OCC)