# Design principles

stddiff.spark is a drop-in replacement for stddiff that runs on Spark. Check behaviour changes against these principles.

## 1. Match stddiff exactly

Local data frames are passed straight to stddiff. Any difference in the Spark implementation would therefore make the same call give different results depending on where the data lives. So Spark results replicate stddiff's behaviour, including its odd behaviour on unusual input:

- Non-binary variables in `stddiff.binary()` give stddiff's results, meaningless as they are. They don't produce an error, an NA or a cardinality limit. Values are counted in Spark and collected, so a high-cardinality column passed by mistake is slow or fails; that's accepted, since its result is meaningless anyway.
- NaN counts as missing in every function. sparklyr's `copy_to()` turns some R NAs into NaN, and NaN is never a real category.
- The group column may also be listed as a variable, and repeated variables give repeated rows.
- No column names or values are reserved.
- Groups and levels are ordered as `as.factor()` orders them by default. Spark has no factor type, so a custom level order can't be expressed.

Parity is about realistic input: don't reimplement R internals to match R on formats that don't occur in practice. Parity doesn't extend to stddiff's integer overflow: with more than about 46,000 rows per group its confidence limits are NA, and the Spark version computes them.

stddiff itself is never wrapped or patched. Its quirks are documented, not fixed.

Parity checks go in the parity table; only deliberate differences get their own tests.

## 2. Accept anything stddiff accepts; stop only for a wrong call

Every column type stddiff can process is accepted. Where stddiff stops because of a variable's values, such as a singular matrix in `stddiff.category()`, that variable gets NA with a warning, and the other variables still get results. Only a wrong call stops: a column of a type stddiff can't process either, such as an array, or a group column without exactly two values.

## Scope

Only what stddiff does. Features it lacks, such as weights, belong in other tools.
