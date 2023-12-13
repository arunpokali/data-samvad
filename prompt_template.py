
# spark SQL few shot examples

# Example 1
sql_question1 = """
QUESTION: Given a Spark temp view `spark_ai_temp_view_14kjd0` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
```
(a, string, [Kongur Tagh, Grossglockner])
(b, int, [7649, 3798])
(c, string, [China, Austria])
```
Write a Spark SQL query to retrieve from view `spark_ai_temp_view_14kjd0`: Find the mountain located in Japan.
"""

sql_answer1 = "SELECT `Name` FROM `spark_ai_temp_view_14kjd0` WHERE `Location` = 'China'"

# Example 2
sql_question2 = """QUESTION: Given a Spark temp view `spark_ai_temp_view_12qcl3` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
(Student, string, [Alice, Bob])
(Birthday, string, [2005-12-12, 2006-03-04])
```
Write a Spark SQL query to retrieve from view `spark_ai_temp_view_12qcl3`: How many students have their birthday in 2006?
"""

sql_answer2 = "SELECT COUNT(`Student`) FROM `spark_ai_temp_view_12qcl3` WHERE YEAR(`Birthday`) = 2006"

# Example 3
sql_question3 = """
QUESTION: Given a Spark temp view `spark_sales_view_91xj5` with the following sample data, in the format (column_name, type, [sample_value_1, sample_value_2...]):
```
(Product, string, [Laptop, Smartphone])
(Sales, int, [300, 450])
(Year, int, [2021, 2022])
```
Write a Spark SQL query to retrieve from view `spark_sales_view_91xj5`: Which product had the highest sales in 2021?"""

sql_answer3 = "SELECT `Product` FROM `spark_sales_view_91xj5` WHERE `Year` = 2021 ORDER BY `Sales` DESC LIMIT 1"

# Example 4
sql_question4 = """
QUESTION: Given a Spark temp view `spark_employee_view_47zc8` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
(Employee, string, [John Doe, Jane Smith])
(Department, string, [Engineering, HR])
(Salary, int, [70000, 50000])
```

Write a Spark SQL query to retrieve from view `spark_employee_view_47zc8`: What is the average salary in the Engineering department?
"""

sql_answer4 = "SELECT AVG(`Salary`) FROM `spark_employee_view_47zc8` WHERE `Department` = 'Engineering'"

# Example 5
sql_question5 = """
QUESTION: Given two Spark temp views `spark_product_view_a1` and `spark_sales_view_b2` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
View `spark_product_view_a1`:
(ProductID, string, [P001, P002])
(Category, string, [Electronics, Furniture])

View `spark_sales_view_b2`:
(ProductID, string, [P001, P002])
(Sales, int, [300, 450])
(Year, int, [2021, 2022])
```

Write a Spark SQL query to retrieve: List all products in the 'Electronics' category with their total sales in 2021 from these views."""

sql_answer5 = "SELECT a.ProductID, SUM(b.Sales) FROM spark_product_view_a1 a JOIN spark_sales_view_b2 b ON a.ProductID = b.ProductID WHERE a.Category = 'Electronics' AND b.Year = 2021 GROUP BY a.ProductID"

# Example 6
sql_question6 = """QUESTION: Given two Spark temp views `spark_employee_view_x9` and `spark_department_view_y7` ith the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
View `spark_employee_view_x9`:
(EmployeeID, string, [E01, E02])
(EmployeeName, string, [John Doe, Jane Smith])
(DepartmentID, string, [D01, D02])

View `spark_department_view_y7`:
(DepartmentID, string, [D01, D02])
(DepartmentName, string, [Engineering, HR])
(Location, string, [New York, San Francisco])
```

Write a Spark SQL query to retrieve: Find the names of all employees working in the 'HR' department, along with their department location."""

sql_answer6 = "SELECT e.EmployeeName, d.Location FROM spark_employee_view_x9 e JOIN spark_department_view_y7 d ON e.DepartmentID = d.DepartmentID WHERE d.DepartmentName = 'HR'"

# Example 7 - Date Formatting
sql_question7 = """QUESTION: Given a Spark temp view `spark_events_view_58zr3` ith the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):

``` 
(EventID, string, [E001, E002]) 
(EventDate, string, [2021-05-20, 2022-08-15]) 
(Location, string, [New York, San Francisco]) 
```

Write a Spark SQL query to retrieve from view `spark_events_view_58zr3`: List all events that occurred in 2021 with their dates formatted as 'MM/dd/yyyy'."""

sql_answer7 = "SELECT EventID, DATE_FORMAT(TO_DATE(`EventDate`), 'MM/dd/yyyy') AS FormattedDate FROM `spark_events_view_58zr3` WHERE YEAR(TO_DATE(`EventDate`)) = 2021"""

# Example 8 - Trend Finding
sql_question8 = """
QUESTION: Given a Spark temp view `spark_sales_view_47qd9` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
 (Product, string, [Laptop, Smartphone]) 
 (MonthlySales, int, [300, 450]) 
 (Month, string, [2021-01, 2021-02]) 
 ```
 
 Write a Spark SQL query to retrieve from view `spark_sales_view_47qd9`: Identify the month with the highest increase in sales compared to the previous month."""

sql_answer8 = "WITH SalesDifferences AS (SELECT Month, MonthlySales, LAG(MonthlySales, 1) OVER (ORDER BY Month) AS PreviousMonthSales FROM `spark_sales_view_47qd9`) SELECT Month, MonthlySales - PreviousMonthSales AS SalesDifference FROM SalesDifferences ORDER BY SalesDifference DESC LIMIT 1"

# Additional Trend-Related Spark SQL Examples

# Example 9 - Yearly Sales Trend
sql_question9 = """
QUESTION: Given a Spark temp view `spark_annual_sales_view_32kl8` with the following sample vals, in the format (column_name, type, [sample_value_1, sample_value_2...]):
``` 
(Product, string, [Laptop, Smartphone]) 
(AnnualSales, int, [1000, 1500]) 
(Year, int, [2020, 2021]) 
```
Write a Spark SQL query to retrieve from view `spark_annual_sales_view_32kl8`: Show the trend of yearly sales for each product."""

sql_answer9 = "SELECT Year, Product, AnnualSales FROM `spark_annual_sales_view_32kl8` ORDER BY Product, Year"

# Example 10 - Quarterly Growth Rate
sql_question10 = """
QUESTION: Given a Spark temp view `spark_quarterly_growth_view_78jr9`  with the following sample vals,
    in the format (column_name, type, [sample_value_1, sample_value_2...]):
    ``` 
    (Quarter, string, [2021-Q1, 2021-Q2]) 
    (GrowthRate, float, [0.05, 0.07]) 
    ```
    Write a Spark SQL query to retrieve from view `spark_quarterly_growth_view_78jr9`: Identify the quarter with the highest growth rate."""

sql_answer10 = "SELECT Quarter FROM `spark_quarterly_growth_view_78jr9` ORDER BY GrowthRate DESC LIMIT 1"

# Compile all examples into a list
SQL_CHAIN_EXAMPLES = [
    sql_question1 + f"\nAnswer:\n```{sql_answer1}```",
    sql_question2 + f"\nAnswer:\n```{sql_answer2}```",
    sql_question3 + f"\nAnswer:\n```{sql_answer3}```",
    # sql_question4 + f"\nAnswer:\n```{sql_answer4}```",
    # sql_question5 + f"\nAnswer:\n```{sql_answer5}```",
    # sql_question6 + f"\nAnswer:\n```{sql_answer6}```",
    # sql_question7 + f"\nAnswer:\n```{sql_answer7}```",
    # sql_question8 + f"\nAnswer:\n```{sql_answer8}```",
    # sql_question9 + f"\nAnswer:\n```{sql_answer9}```",
    # sql_question10 + f"\nAnswer:\n```{sql_answer10}```"
]



SPARK_SQL_SUFFIX="""

Sample examples finished. 

List of Spark views : {views}
Following are sample values in temp views, in the format (column_name, type, [sample_value_1, sample_value_2...]):
```
{sample_data_per_view}
```

Now, Write a Spark SQL query for below question, Remember for given a question, you need to provide only Spark SQL query to answer the question, no code to run query is required. Also, It's very important to ONLY use the verbatim column name in your resulting SQL query; DO NOT include the type. 

Strictly provide only spark sql query in below format, replace <sql-query> :
``` 
<sql-query>
```
explain - query formation strategy in less than 150 words


Question: {desc}

"""


SPARK_SQL_PREFIX = """You are an assistant for writing professional Spark SQL queries. 
Given a question, you need to write a Spark SQL query to answer the question. The result is ALWAYS a Spark SQL query.

Keep in mind the following 8 guidelines for writing Spark SQL query :

1. Use the COUNT SQL function when the query asks for the total number of entries in a non-countable column (e.g., names, categories).

2. Use the SUM SQL function to accumulate totals of countable column values (e.g., sales, quantities).

3. Handling Aggregations and Groupings: If the question involves summarizing data, employ GROUP BY clauses along with aggregation functions like SUM, AVG, MAX, MIN.

4. Consideration of Joins: When the question implies combining data from multiple views, use appropriate join types (INNER, LEFT, RIGHT, FULL) based on the context.

5. Handling Date and Time: If the query involves date or time-based data, incorporate relevant functions to format, extract, or calculate date/time values as required.

When crafting SQL queries involving multiple views, especially those with JOIN operations, it's crucial to use proper aliasing for clarity and to prevent errors. Aliasing becomes vital when dealing with columns that share names across different views. Here are some key practices:

Assign Aliases to Each View: Give every view in your query a unique alias. This enhances readability and clarifies which view a column belongs to. For instance, use FROM view1 AS v1 and FROM view2 AS v2 for two different views.
Alias Common Column Names: When views share column names, alias these columns distinctly in your SELECT statements and JOIN conditions. This approach eliminates ambiguity. For example, SELECT v1.id AS v1_id, v2.id AS v2_id clearly differentiates the id columns from each view.

Here are some sample Spark Sql queries.

"""


PLOT_PROMPT_TEMPLATE = """
Given a pandas DataFrame instance `df`, with the columns: {columns}

Write Python code to visualize the result of `df` using plotly:

2. Make sure to use the exact column names of `df`.
3. Your code may NOT contain "append" anywhere. Instead of append, use pd.concat.
4. There is no need to install any package with pip. Do include any necessary import statements.
5. Save the plot as png image with filename as {image_name} in the directory plot_images.
6. Do not use scatter plot to display any kind of percentage data.
7. It is forbidden to include old deprecated APIs in your code.
8. Ensure that your code is correct.

{instruction}
"""