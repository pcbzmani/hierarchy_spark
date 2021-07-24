# hierarchy_spark
Find the hierarchy of employee in three levels

## Input dataset:

EmployeeID	SupervisiorID
1	2
2	4
8	6
9	5
6	3
5	10
4	17
3	15
10	20
15	20
17	20
16	21
14	12
13	11
![image](https://user-images.githubusercontent.com/17996905/126861365-0868bae1-ff5b-4c3c-acb5-4a14c7c76e56.png)

|EmployeeID|SupervisiorID|
|----------|-------------|
|1|2|
|2|4|
|8|6|
|9|5|
|6|3|
|5|10|
|4|17|
|3|15|
|10|20|
|15|20|
|17|20|
|16|21|
|15|13|
|14|12|
|13|11|

## Expected output:
EmployeeID	SupervisiorID_1	SupervisiorID_2	SupervisiorID_3	SupervisiorID_4	SupervisiorID_5
1	2	4	17	20	20
2	4	17	20	20	20
8	6	3	15	20	20
9	5	10	20	20	20
6	3	15	20	20	20
5	10	20	20	20	20
4	17	20	20	20	20
3	15	20	20	20	20
10	20	20	20	20	20
15	20	20	20	20	20
17	20	20	20	20	20
16	21	21	21	21	21
14	12	12	12	12	12
13	11	11	11	11	11
![image](https://user-images.githubusercontent.com/17996905/126861380-c7f0a0cd-cfbc-4ccb-be41-f542367e1da4.png)

code is in hierarchy.py in code folder
