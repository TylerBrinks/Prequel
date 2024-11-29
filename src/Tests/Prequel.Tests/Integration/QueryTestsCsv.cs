using Prequel.Engine.Core.Data;
using Prequel.Engine.Core.Logical;
using Prequel.Engine.IO;
using Prequel.Engine.Core.Data;
using Prequel.Engine.Source.Csv;
using Exec = Prequel.Engine.Core.Execution.ExecutionContext;
namespace Prequel.Tests.Integration;

public class QueryTestsCsv
{
    private readonly Exec _context;

    public QueryTestsCsv()
    {
        _context = new Exec();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/');
        Task.Run(async () =>
        {
            await _context.RegisterCsvFileAsync("departments", new LocalFileStream($"{root}/Integration/db_departments.csv"));
            await _context.RegisterCsvFileAsync("employees", new LocalFileStream($"{root}/Integration/db_employees.csv"));
            await _context.RegisterCsvFileAsync("jobs", new LocalFileStream($"{root}/Integration/db_jobs.csv"));
            await _context.RegisterCsvFileAsync("locations", new LocalFileStream($"{root}/Integration/db_locations.csv"));
            await _context.RegisterCsvFileAsync("countries", new LocalFileStream($"{root}/Integration/db_countries.csv"));
        }).Wait();
    }

    private async Task<RecordBatch> ExecuteSingleBatchAsync(string sql, QueryContext? options = null)
    {
        var execution = _context.ExecuteQueryAsync(sql, options ?? new QueryContext());

        var enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        return enumerator.Current;
    }

    [Fact]
    public async Task Query_Handles_Empty_Relations()
    {
        var batch = await ExecuteSingleBatchAsync("select 1 as a, 2 as b");
        Assert.Equal(1, batch.RowCount);
        Assert.Equal("a", batch.Schema.Fields[0].QualifiedName);
        Assert.Equal("b", batch.Schema.Fields[1].QualifiedName);
        Assert.Equal((byte)2, batch.Results[1].Values[0]);
        Assert.Equal((byte)1, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Ignores_Field_Name_Case()
    {
        var batch = await ExecuteSingleBatchAsync("select employee_id, Employee_Id, EMPLOYEE_ID from employees");
        Assert.Equal(107, batch.RowCount);
    }

    [Fact]
    public async Task Query_Groups_Aggregated_Values_By_Column()
    {
        var sql = """
            select distinct department_id
            from employees
            """;
        var batch = await ExecuteSingleBatchAsync(sql);
        Assert.Equal(12, batch.RowCount);

        sql = """
            select avg(salary)
            from employees
            group by department_id
            """;
        batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(12, batch.RowCount);
    }

    [Fact]
    public async Task Query_Groups_Multiple_Column()
    {
        const string sql = """
            select department_id, job_id, avg(salary)
            from employees
            group by department_id, job_id
            """;
        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(20, batch.RowCount);
    }

    [Fact]
    public async Task Query_Grouped_Aggregated_Values_With_Filter()
    {
        const string sql = """
            select avg(salary)
            from employees
            group by department_id
            having department_id > 90
            """;
        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(2, batch.RowCount);
    }

    [Fact]
    public async Task Query_Grouped_Aggregated_Values_With_Filter_And_Limit()
    {
        const string sql = """
            select avg(salary)
            from employees
            group by department_id
            having department_id > 90
            limit 1
            """;
        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(1, batch.RowCount);
    }

    [Fact]
    public async Task Query_Counts_Table_Records()
    {
        var batch = await ExecuteSingleBatchAsync("select count(employee_id) from employees");

        Assert.Equal(1, batch.RowCount);
        Assert.Equal((byte)107, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Evaluates_Numeric_Filters()
    {
        var sql = """
                SELECT first_name, last_name
                FROM employees 
                WHERE salary > 10000
                """;
        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(15, batch.RowCount);

        sql = """
                SELECT count(*)
                FROM employees 
                WHERE salary > 10000
                """;
        batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(1, batch.RowCount);
        Assert.Equal((byte)15, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Evaluates_Aliased_Self_Join()
    {
        const string sql = """
                SELECT 
                    mgr.employee_id as MgrId, 
                    emp.employee_id, 
                    mgr.first_name MgrFirst, 
                    mgr.last_name MgrLast,
                    emp.first_name EmpFirst, 
                    emp.last_name EmpLast
                FROM employees mgr
                join employees emp
                on mgr.employee_id = emp.manager_id
                where emp.manager_id = 100
                order by emp.manager_id
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(14, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Join_With_Filters()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                join departments d
                on e.department_id > d.department_id
                where d.department_name = 'Sales'
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(11, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Join_With_Using()
    {
        const string sql = """
                SELECT
                    count(e.employee_id)
                FROM employees e
                join departments d
                USING(department_id)
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(1, batch.RowCount);
        Assert.Equal((byte)106, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Evaluates_Join_With_Multiple_Conditions()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                join departments d
                on e.department_id = d.department_id and
                e.manager_id = d.manager_id
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(32, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Left_Join()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                left join departments d
                on e.department_id = d.department_id
                where d.department_name = 'Sales'
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(34, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Left_Join_With_Missing_Right_Data()
    {
        var schema = new Schema([new("empty_id", ColumnDataType.Integer, new TableReference("empty"))]);

        _context.RegisterDataTable(new EmptyDataTable("empty", schema, [0]));

        // Left join without right data will return the entire left data set

        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                left join empty
                on e.department_id = empty.empty_id
                """;

        var batch = await ExecuteSingleBatchAsync(sql);
        Assert.Equal(107, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Left_Semi_Join_With_Missing_Right_Data()
    {
        var schema = new Schema([new("empty_id", ColumnDataType.Integer, new TableReference("empty"))]);

        _context.RegisterDataTable(new EmptyDataTable("empty", schema, new List<int> { 0 }));

        // Left join without right data will return the entire left data set

        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                left semi join empty
                on e.department_id = empty.empty_id
                """;

        var batch = await ExecuteSingleBatchAsync(sql);
        Assert.Equal(0, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Left_Outer_Join()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                left outer join departments d
                on e.department_id = d.department_id
                where d.department_name = 'Sales'
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(34, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Right_Outer_Join()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                inner join departments d
                on e.department_id = d.department_id
                where d.department_name = 'Sales'
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(34, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Full_Join()
    {
        const string sql = """
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name
                FROM employees e
                full join departments d
                on e.department_id = d.department_id
                where d.department_name = 'Sales'
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(34, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Subquery_Records()
    {
        const string sql = """
            SELECT *
            FROM 
                (SELECT 
                    employee_id, 
                    first_name,
                    last_name
                FROM employees)
            """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);
        Assert.Equal(3, batch.Results.Count);
    }

    [Fact]
    public async Task Query_Evaluates_Single_Parameter_Aggregations()
    {
        const string sql = """
            SELECT 
                count(salary), sum(salary),
                max(salary), min(salary),
                avg(salary), mean(salary), median(salary),
                var(salary), var_samp(salary), var_pop(salary), 
                stddev(salary), stddev_samp(salary), stddev_pop(salary)
            FROM employees
            """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(1, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Multi_Parameter_Aggregations()
    {
        const string sql = """
            SELECT 
                covar(salary,employee_id), covar_samp(salary,employee_id), covar_pop(salary,employee_id)
            FROM employees
            """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(1, batch.RowCount);
    }

    [Fact]
    public async Task Query_Sorts_Multiple_Columns()
    {
        const string sql = """
                SELECT 
                    first_name, 
                    last_name
                FROM employees
                order by first_name, last_name desc
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);
    }

    [Fact]
    public async Task Query_Limit_And_Offset_Filter_Results()
    {
        const string sql = """
                SELECT 
                    employee_id
                FROM employees
                order by employee_id
                limit 10
                offset 20
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(10, batch.RowCount);
        Assert.Equal((byte)120, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Short_Circuits_Zero_Limit()
    {
        const string sql = """
                SELECT 
                    employee_id
                FROM employees
                order by employee_id
                limit 0
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Null(batch);
    }

    [Fact]
    public async Task Query_Ignores_Limits()
    {
        var sql = $"""
                SELECT 
                    employee_id
                FROM employees
                order by employee_id
                offset 0
                limit {int.MaxValue}
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);
    }

    [Fact]
    public async Task Query_Orders_Multiple_Columns()
    {
        const string sql = """
                SELECT 
                    last_name,
                    first_name
                FROM employees
                order by last_name, first_name
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);
        Assert.Equal("Abel", batch.Results[0].Values[0]);
        Assert.Equal("Ande", batch.Results[0].Values[1]);
        Assert.Equal("Ellen", batch.Results[1].Values[0]);
        Assert.Equal("Sundar", batch.Results[1].Values[1]);
    }

    [Fact]
    public async Task Query_Calculates_Values()
    {
        const string sql = """
                SELECT 
                    salary,
                    salary * salary
                FROM employees
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);

        for (var i = 0; i < batch.RowCount; i++)
        {
            var salary = (double)batch.Results[0].Values[i]!;
            var squared = (double)batch.Results[1].Values[i]!;

            Assert.Equal(salary * salary, squared);
        }
    }

    [Fact]
    public async Task Query_Handles_Invalid_Double_Values()
    {
        const string sql = """
                SELECT 
                    salary / 0
                FROM employees
                """;

        var batch = await ExecuteSingleBatchAsync(sql);

        Assert.Equal(107, batch.RowCount);

        for (var i = 0; i < batch.RowCount; i++)
        {
            Assert.Equal(double.PositiveInfinity, (double)batch.Results[0].Values[i]!);
        }
    }

    [Fact]
    public async Task Query_Cannot_Handle_Invalid_Int_Values()
    {
        const string sql = """
                SELECT 
                    employee_id / 0
                FROM employees
                """;

        await Assert.ThrowsAsync<DivideByZeroException>(() => ExecuteSingleBatchAsync(sql));
    }

    [Fact]
    public async Task Query_Executes_Cross_Joins_From_Subquery()
    {
        const string sql = """
            SELECT * 
            FROM employees e where salary < (
                SELECT AVG(salary) 
                FROM employees
                where employee_id > 110
            )
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(50, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Cross_Joins_With_Outer_Ref()
    {
        const string sql = """
            SELECT * 
            FROM employees e where salary < (
                SELECT AVG(salary) 
                FROM employees
                where employee_id < e.employee_id
                AND
                salary > 21000 
            )
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(106, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Cross_Join_Expressions_With_Aliases()
    {
        const string sql = """
            SELECT 
                e.department_id, e.first_name, e.job_id, d.department_name  
            FROM 
                employees e, departments d  
            WHERE 
                e.department_id = d.department_id  
            AND 
                d.department_name = 'Finance'
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(6, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Cross_Joins()
    {
        const string sql = """
            SELECT 
                e.*
            FROM 
                employees e
            CROSS JOIN
                jobs j
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(2033, rowCount);
    }

    [Fact]
    public async Task Query_Executes_In_Filter()
    {
        const string sql = """
            SELECT 
                first_name  
            FROM 
                employees
            WHERE 
                department_id in (90, 60) 
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(8, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Negated_In_Filter()
    {
        const string sql = """
            SELECT 
                first_name  
            FROM 
                employees
            WHERE 
                department_id not in (90, 60) 
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(99, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Between_As_Filters()
    {
        const string sql = """
            SELECT 
                *  
            FROM 
                departments
            WHERE 
                department_id between 90 and 130
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(5, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Not_Between_As_Filters()
    {
        const string sql = """
            SELECT 
                *  
            FROM 
                departments
            WHERE 
                department_id not between 90 and 130
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(22, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Between_Dates()
    {
        const string sql = """
            SELECT 
                *  
            FROM 
                employees
            WHERE 
                hire_date between '2004-01-01' and '2005-12-31'
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(39, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Like_Expressions_Case_Sensitive()
    {
        var sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                country_name like '%ma%'
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(2, rowCount);

        sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                country_name ilike '%ma%'
            """;

        rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(3, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Not_Like_Expressions_Case_Sensitive()
    {
        var sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                country_name not like '%ma%'
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(23, rowCount);

        sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                country_name not ilike '%ma%'
            """;

        rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(22, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Non_Field_Like_Expressions()
    {
        var sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                'abc' like '_b%'
            """;

        var rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(25, rowCount);

        sql = """
            SELECT 
                country_name 
            FROM 
                countries
            WHERE 
                'abc' not like '_b%'
            """;

        rowCount = 0;
        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(0, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Case_Statements_With_Expressions()
    {
        const string sql = """
            SELECT 
            CASE region_id 
                WHEN 1 THEN 'europe' 
                WHEN 2 THEN 'south america' 
                WHEN 3 THEN 'asia'
                ELSE 'africa' 
            END 
            FROM countries
            """;

        var batch = await ExecuteSingleBatchAsync(sql);
        var countries = batch.Results[0].Values.Cast<string>().GroupBy(_ => _).ToList();

        Assert.Equal(25, batch.RowCount);
        Assert.Equal(8, countries.First(c => c.Key == "europe").Count());
        Assert.Equal(5, countries.First(c => c.Key == "south america").Count());
        Assert.Equal(6, countries.First(c => c.Key == "asia").Count());
        Assert.Equal(6, countries.First(c => c.Key == "africa").Count());
    }

    [Fact]
    public async Task Query_Executes_Case_Statements_Without_Expressions()
    {
        const string sql = """
            SELECT 
            CASE 
                WHEN region_id = 1 THEN 'europe' 
                WHEN region_id = 2 THEN 'south america' 
                WHEN region_id = 3 THEN 'asia'
                ELSE 'africa' 
            END 
            FROM countries
            """;

        var batch = await ExecuteSingleBatchAsync(sql, new QueryContext());
        var countries = batch.Results[0].Values.Cast<string>().GroupBy(_ => _).ToList();

        Assert.Equal(25, batch.RowCount);
        Assert.Equal(8, countries.First(c => c.Key == "europe").Count());
        Assert.Equal(5, countries.First(c => c.Key == "south america").Count());
        Assert.Equal(6, countries.First(c => c.Key == "asia").Count());
        Assert.Equal(6, countries.First(c => c.Key == "africa").Count());
    }

    [Fact]
    public async Task Query_Executes_Combined_Unions()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            UNION ALL
            SELECT job_id FROM jobs
            """;

        var rowCount = 0;

        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(count * 2, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Distinct_Unions()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            UNION
            SELECT job_id FROM jobs
            """;

        var rowCount = 0;

        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
        }

        Assert.Equal(count, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Multiple_Combined_Unions()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            UNION ALL
            SELECT job_id FROM jobs
            UNION ALL
            SELECT job_id FROM jobs
            UNION ALL
            SELECT job_id FROM jobs
            """;

        var rowCount = 0;
        var batches = 0;

        await foreach (var batch in _context.ExecuteQueryAsync(sql))
        {
            rowCount += batch.RowCount;
            batches++;
        }

        Assert.Equal(count * 4, rowCount);
        Assert.Equal(4, batches);
    }

    [Fact]
    public async Task Query_Executes_Multiple_Merged_Unions()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            UNION
            SELECT job_id FROM jobs
            UNION
            SELECT job_id FROM jobs
            UNION
            SELECT job_id FROM jobs
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(count, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Combined_Intersection()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            INTERSECT ALL
            SELECT job_id FROM jobs
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(count, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Merged_Intersection()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            INTERSECT
            SELECT job_id FROM jobs
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(count, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Fixed_Intersection()
    {
        const string sql = """
            SELECT job_id FROM jobs
            INTERSECT
            SELECT 'AD_PRES' as job_id
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(1, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Combined_Exception()
    {
        const string sql = """
            SELECT job_id FROM jobs
            EXCEPT
            SELECT job_id FROM jobs
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(0, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Merged_Exception()
    {
        const string sql = """
            SELECT job_id FROM jobs
            EXCEPT ALL
            SELECT job_id FROM jobs limit 10
            """;

        Assert.Null(await ExecuteSingleBatchAsync(sql));
    }

    [Fact]
    public async Task Query_Executes_Fixed_Exception()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT job_id FROM jobs
            EXCEPT
            SELECT 'AD_PRES' as job_id
            """;

        var rowCount = (await ExecuteSingleBatchAsync(sql)).RowCount;

        Assert.Equal(count - 1, rowCount);
    }

    [Fact]
    public async Task Query_Executes_Subquery_Intersection()
    {
        const string sql = """
            SELECT * FROM (
                SELECT job_id FROM jobs 
                INTERSECT 
                SELECT job_id FROM jobs
                )
            """;

        var batch = await ExecuteSingleBatchAsync(sql);
        Assert.Equal(19, batch.RowCount);
    }

    [Fact]
    public async Task Query_Executes_Subquery_Intersections_With_Exceptions()
    {
        var countBatch = await ExecuteSingleBatchAsync("SELECT count(job_id) FROM jobs");
        var count = Convert.ToInt32((countBatch.Results[0] as ByteArray)!.Values[0]);

        const string sql = """
            SELECT * FROM (
                SELECT job_id FROM jobs 
                INTERSECT 
                SELECT job_id FROM jobs
                )
            EXCEPT
            SELECT 'AD_PRES' as job_id
            """;

        var batch = await ExecuteSingleBatchAsync(sql);
        Assert.Equal(count - 1, batch.RowCount);
    }
}

