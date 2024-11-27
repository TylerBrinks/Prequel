using Spectre.Console;
using Prequel.Model.Execution;

namespace Prequel.Console;
internal static class Results
{
    internal static void Display(QueryResult result)
    {

        foreach (var cellResult in result.QueryResults)
        {
            AnsiConsole.MarkupLine($"[green]{cellResult.Query}[/]");
            AnsiConsole.MarkupLine($"[yellow]{cellResult.Timing!.DurationMilliseconds}ms[/]");

            var table = new Table();
            var columnsAdded = false;

            foreach (var batch in cellResult.Batches)
            {
                if (!columnsAdded)
                {
                    foreach (var field in batch.Schema.Fields)
                    {
                        table.AddColumn(field.Name);
                    }

                    columnsAdded = true;
                }

                for (var i = 0; i < batch.RowCount; i++)
                {
                    var row = batch.Results.Select(value => value.GetStringValue(i).EscapeMarkup() ?? "").ToArray();
                    table.AddRow(row);
                }
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }
    }
}
