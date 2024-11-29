using Prequel.Data;
using SqlParser.Ast;

namespace Prequel
{
    public static class Extensions
    {
        /// <summary>
        /// Converts an enumerable long array into a nullable equivalent
        /// </summary>
        /// <param name="source">Enumerable long instance</param>
        /// <returns>Enumerable nullable long instance</returns>
        internal static long?[] AsNullable(this IEnumerable<long> source)
        {
            return source.Cast<long?>().ToArray();
        }
        /// <summary>
        /// Counts the number of nulls in an enumerable nullable long instance
        /// </summary>
        /// <param name="source">Enumerable nullable long instance</param>
        /// <returns>Null count in the list</returns>
        internal static int NullCount(this IEnumerable<long?> source)
        {
            return source.Count(i => i == null);
        }
        /// <summary>
        /// Gets friendly text for a given binary operator
        /// </summary>
        /// <param name="op">Binary operator</param>
        /// <returns>Friendly binary operator text</returns>
        internal static string GetDisplayText(this BinaryOperator op)
        {
            return op switch
            {
                BinaryOperator.Plus => "+",
                BinaryOperator.Minus => "-",
                BinaryOperator.Multiply => "*",
                BinaryOperator.Divide => "/",
                BinaryOperator.Modulo => "%",
                BinaryOperator.Gt => ">",
                BinaryOperator.Lt => "<",
                BinaryOperator.GtEq => ">=",
                BinaryOperator.LtEq => "<=",
                BinaryOperator.Spaceship => "<=>",
                BinaryOperator.Eq => "=",
                BinaryOperator.NotEq => "!=",
                BinaryOperator.And => "AND",
                BinaryOperator.Or => "OR",
                BinaryOperator.Xor => "XOR",
                BinaryOperator.BitwiseOr => "|",
                BinaryOperator.BitwiseAnd => "&",
                BinaryOperator.BitwiseXor => "^",

                _ => op.ToString().ToUpperInvariant()
            };
        }
        /// <summary>
        /// Duplicates SQL LIKE string comparison behavior by comparing a string value
        /// against a common SQL LIKE pattern.
        /// </summary>
        /// <param name="value">Value to compare</param>
        /// <param name="pattern">SQL LIKE pattern text</param>
        /// <param name="caseSensitive">True for case-sensitive comparison; otherwise false.</param>
        /// <returns>True if the value matches the pattern; otherwise false</returns>
        internal static bool SqlLike(this string value, string pattern, bool caseSensitive = false)
        {
            bool isMatch = true,
                 isWildCardOn = false,
                 isCharWildCardOn = false,
                 isCharSetOn = false,
                 isNotCharSetOn = false,
                 endOfPattern;

            var lastWildCard = -1;
            var patternIndex = 0;
            var set = new List<char>();
            var val = '\0';

            foreach (var character in value)
            {
                endOfPattern = patternIndex >= pattern.Length;

                if (!endOfPattern)
                {
                    val = pattern[patternIndex];

                    if (!isWildCardOn && val == '%')
                    {
                        lastWildCard = patternIndex;
                        isWildCardOn = true;

                        while (patternIndex < pattern.Length && pattern[patternIndex] == '%')
                        {
                            patternIndex++;
                        }

                        val = patternIndex >= pattern.Length ? '\0' : pattern[patternIndex];
                    }
                    else switch (val)
                        {
                            case '_':
                                isCharWildCardOn = true;
                                patternIndex++;
                                break;

                            case '[':
                                {
                                    if (pattern[++patternIndex] == '^')
                                    {
                                        isNotCharSetOn = true;
                                        patternIndex++;
                                    }
                                    else isCharSetOn = true;

                                    set.Clear();

                                    if (pattern[patternIndex + 1] == '-' && pattern[patternIndex + 3] == ']')
                                    {
                                        //var start = char.ToUpper(pattern[patternIndex]);
                                        var start = Normalized(pattern[patternIndex]);
                                        patternIndex += 2;
                                        //var end = char.ToUpper(pattern[patternIndex]);
                                        var end = Normalized(pattern[patternIndex]);

                                        if (start <= end)
                                        {
                                            for (var charIndex = start; charIndex <= end; charIndex++)
                                            {
                                                set.Add(charIndex);
                                            }
                                        }

                                        patternIndex++;
                                    }

                                    while (patternIndex < pattern.Length && pattern[patternIndex] != ']')
                                    {
                                        set.Add(pattern[patternIndex]);
                                        patternIndex++;
                                    }

                                    patternIndex++;
                                    break;
                                }
                        }
                }

                if (isWildCardOn)
                {
                    if (Normalized(character) != Normalized(val))
                    {
                        continue;
                    }

                    isWildCardOn = false;
                    patternIndex++;
                }
                else if (isCharWildCardOn)
                {
                    isCharWildCardOn = false;
                }
                else if (isCharSetOn || isNotCharSetOn)
                {
                    var charMatch = set.Contains(Normalized(character));

                    if (isNotCharSetOn && charMatch || isCharSetOn && !charMatch)
                    {
                        if (lastWildCard >= 0)
                        {
                            patternIndex = lastWildCard;
                        }
                        else
                        {
                            isMatch = false;
                            break;
                        }
                    }

                    isNotCharSetOn = isCharSetOn = false;
                }
                else
                {
                    if (Normalized(character) == Normalized(val))
                    {
                        patternIndex++;
                    }
                    else
                    {
                        if (lastWildCard >= 0) patternIndex = lastWildCard;
                        else
                        {
                            isMatch = false;
                            break;
                        }
                    }
                }
            }

            endOfPattern = patternIndex >= pattern.Length;

            if (!isMatch || endOfPattern)
            {
                return isMatch && endOfPattern;
            }

            var isOnlyWildCards = true;

            for (var i = patternIndex; i < pattern.Length; i++)
            {
                if (pattern[i] == '%')
                {
                    continue;
                }
                isOnlyWildCards = false;
                break;
            }

            if (isOnlyWildCards)
            {
                endOfPattern = true;
            }

            return isMatch && endOfPattern;

            char Normalized(char input)
            {
                return caseSensitive ? input : char.ToUpper(input);
            }
        }
        /// <summary>
        /// Removes leading and trailing single or double quotes
        /// </summary>
        /// <param name="value">Value to trim</param>
        /// <returns>Trimmed string value</returns>
        public static string? TrimQuotes(this string? value)
        {
            if (value == null)
            {
                return value;
            }

            if (value.StartsWith("'"))
            {
                return value.Trim('\'');
            }

            if (value.StartsWith("\""))
            {
                return value.Trim('\"');
            }

            return value;

        }
        /// <summary>
        /// Converts a CLR primitive type into a supported column type
        /// </summary>
        /// <param name="type">Inbuilt .NET type</param>
        /// <returns>Column data type</returns>
        public static ColumnDataType GetColumnType(this Type type)
        {
            return type switch
            {
                not null when type == typeof(bool) => ColumnDataType.Boolean,

                not null when type == typeof(sbyte) ||
                              type == typeof(byte) ||
                              type == typeof(short) ||
                              type == typeof(int) ||
                              type == typeof(long) ||
                              type == typeof(ushort) ||
                              type == typeof(uint) ||
                              type == typeof(ulong) => ColumnDataType.Integer,

                not null when type == typeof(decimal) ||
                              type == typeof(double) ||
                              type == typeof(float) => ColumnDataType.Double,

                not null when type == typeof(DateTime) => ColumnDataType.TimestampNanosecond,

                _ => ColumnDataType.Utf8
            };
        }
        /// <summary>
        /// Converts a Column type into a CLR primitive type
        /// </summary>
        /// <param name="columnType">Inbuilt .NET type</param>
        /// <returns>Column data type</returns>
        public static Type GetPrimitiveType(this ColumnDataType columnType)
        {
            return columnType switch
            {
                ColumnDataType.Boolean => typeof(bool),
                ColumnDataType.Integer => typeof(long),
                ColumnDataType.Double => typeof(double),
                ColumnDataType.Date32
                    or ColumnDataType.TimestampSecond
                    or ColumnDataType.TimestampNanosecond => typeof(DateTime),

                ColumnDataType.Utf8 => typeof(string),
                _ => typeof(string),
            };
        }

        /// <summary>
        /// Converts a Column type into a CLR primitive type
        /// </summary>
        /// <param name="columnType">Inbuilt .NET type</param>
        /// <returns>Column data type</returns>
        public static Type GetNullablePrimitiveType(this ColumnDataType columnType)
        {
            return columnType switch
            {
                ColumnDataType.Boolean => typeof(bool?),
                ColumnDataType.Integer => typeof(long?),
                ColumnDataType.Double => typeof(double?),
                ColumnDataType.Date32
                    or ColumnDataType.TimestampSecond
                    or ColumnDataType.TimestampNanosecond => typeof(DateTime?),

                ColumnDataType.Utf8 => typeof(string),
                _ => typeof(string),
            };
        }
        /// <summary>
        /// Checks if a string is a numeric value
        /// </summary>
        /// <param name="value">Value to parse</param>
        /// <returns>True if the value is parsed as a numeric value; otherwise false.</returns>
        public static (bool IsNumeric, Type? NumericType) ParseNumeric(this string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return (false, null);
            }

            var isNumeric = byte.TryParse(value, out _);
            if (isNumeric)
            {
                return (true, typeof(byte));
            }

            isNumeric = short.TryParse(value, out _);
            if (isNumeric)
            {
                return (true, typeof(short));
            }

            isNumeric = int.TryParse(value, out _);
            if (isNumeric)
            {
                return (true, typeof(int));
            }

            isNumeric = long.TryParse(value, out _);
            if (isNumeric)
            {
                return (true, typeof(long));
            }

            return (false, null);
        }
        /// <summary>
        /// Wraps an enumerable list in an async method to yield records
        /// using an IAsyncEnumerable result
        /// </summary>
        /// <typeparam name="T">Generic list type</typeparam>
        /// <param name="list">List to enumerate</param>
        /// <returns>IAsyncEnumerable list</returns>
        public static async IAsyncEnumerable<T> ToIAsyncEnumerable<T>(this IEnumerable<T> list)
        {
            await Task.CompletedTask;

            foreach (var item in list)
            {
                yield return item;
            }
        }
        /// <summary>
        /// Compare two objects with unknown types for value equality.  Numbers
        /// are converted to a type that can be safely compared.
        /// </summary>
        /// <param name="left">Left value to compare</param>
        /// <param name="right">Right value to compare</param>
        /// <returns>True if equal; otherwise false</returns>
        public static bool CompareValueEquality(this object? left, object? right)
        {
            if (left == null && right == null)
            {
                return true;
            }

            if (left != null && right == null || left == null && right != null)
            {
                return false;
            }

            if (left is byte or short or int or long && right is byte or short or int or long)
            {
                return Convert.ToInt64(left).Equals(Convert.ToInt64(right));
            }

            return left!.Equals(right);
        }
    }
}