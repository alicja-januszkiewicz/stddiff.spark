# stddiff.spark 1.1.0

## Dependencies

* Requires sparklyr 1.8.4 or later (was 1.8.0), as earlier releases don't work
  with current versions of dbplyr.

* sparklyr releases before 1.9.5 need dbplyr below 2.6.0.

* Suggests stddiff 3.1 or later (was 2.1.0), whose missing counts the Spark
  versions match.

## Bug fixes

* Column names and group values that clash with internal columns now work.
  Some, such as a group column named `n` or `x`, caused errors or wrong
  results, including swapped groups.

* `stddiff.binary()` and `stddiff.category()` now sort numbers numerically, as
  `stddiff` does. 10 sorted before 2, changing the level order and `p.c`/`p.t`.

* Numbers equal to 15 significant digits, such as `0.1 + 0.2` and `0.3`, now
  share a group or level, as in `stddiff`.

* Infinite values now sort as numbers and are labelled `-Inf` and `Inf` in
  `stddiff.binary()` and `stddiff.category()`, as in `stddiff`.

* A column repeated in `vcol` now gives repeated rows, as in `stddiff`. It
  failed with an error.

* A `vcol` that includes the group column now gives `stddiff`'s results.

* `NaN` now counts as missing in every function, as it did in 1.0 for binary
  variables. sparklyr's `copy_to()` turns some R `NA`s into `NaN`.

* `stddiff.numeric()` now reports a group with no non-missing values as
  `stddiff` does, with the other group in the `.c` columns.

* `stddiff.category()` now takes each variable's levels from the whole column,
  including rows whose group is missing, as `stddiff` does.

* `stddiff.category()` now shows `DecimalType` levels as numbers in row names.

## Other changes

* `stddiff.numeric()` now accepts logical columns, coded 0/1, and string
  columns, converted as `as.numeric()` converts them, also in ANSI mode.

* Unusual number formats that `as.numeric()` reads, such as hexadecimal
  (`"0x1A"`), become `NA` with the coercion warning in `stddiff.numeric()`.

* `stddiff.binary()` now gives `stddiff`'s results for variables with more
  than two values, including R's "NaNs produced" warnings.

* Where `stddiff` stops with an error because of one variable, that variable
  gets `NA` with a warning, and the other variables still get results.

* The functions no longer warn about double-typed columns. The group column
  may be of any type they accept, and must have exactly two values.

* Dates and date-times are accepted, as variables and as the group column.
  `stddiff.numeric()` converts them to days and seconds since 1970.

* Calendar intervals and `NULL` columns are accepted, as the strings and `NA`s
  `collect()` gives for them. Arrays, maps, structs and other types that
  `stddiff` can't process after `collect()` stop with an error.

* With more than about 46,000 rows per group, the confidence limits are
  computed where `stddiff`'s integer counts overflow and give `NA`.

## Documentation

* `?stddiff.numeric` no longer says the result equals Cohen's d with pooled SD.

* The README explains that the group order only decides which group is
  reported as control, and states the sparklyr and dbplyr requirements.

* A "Differences from stddiff" section in the README, with notes in the help
  pages, lists the differences on Spark data.

* The README and `?stddiff.binary` note that, like `stddiff`, it doesn't check
  that variables are binary.

* `?stddiff.binary` and `?stddiff.category` warn against continuous or ID-like
  columns, as each distinct value is collected.

# stddiff.spark 1.0

* Initial CRAN submission.
