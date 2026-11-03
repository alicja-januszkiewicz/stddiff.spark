[![CRAN status](https://www.r-pkg.org/badges/version/stddiff.spark)](
https://CRAN.R-project.org/package=stddiff.spark
)
[![R-CMD-check](https://github.com/alicja-januszkiewicz/stddiff.spark/actions/workflows/r-check.yml/badge.svg)](https://github.com/alicja-januszkiewicz/stddiff.spark/actions/workflows/r-check.yml)

# Introduction

`stddiff.spark` provides Spark-compatible implementations of the standardized difference calculations from the `stddiff` package. The interface is identical to `stddiff`, so you can swap your existing calls in-place without changing your workflow.  

Functions automatically dispatch to the `stddiff` package when non-Spark data is supplied, so the same code works seamlessly on both local R data frames and Spark DataFrames.

## Ordering of groups and levels

Spark DataFrames have no factor type, so values are ordered the way `stddiff`'s default `as.factor()` conversion orders them: numbers in numeric order, strings alphabetically, dates and date-times in time order, and `FALSE` before `TRUE`.

- **Group column (`gcol`)**: the first value is reported as the control group (the `.c` columns) and the second as the treatment group (the `.t` columns).
- **Binary variables**: the first value is coded 0 and the second 1, so `p.c` and `p.t` are the proportions of the second value.
- **Categorical variables**: levels are listed in this order, and the first level is the reference.

The ordering only decides how results are labeled. The standardized differences and their confidence intervals are the same either way: the numeric and binary differences are absolute values, and the categorical difference is a Mahalanobis distance, which doesn't depend on which group comes first or which level is the reference.

> [!Note]
> To report a different group as the control group, recode the group column so that group sorts first. For example, if the groups are "drug" and "placebo", "drug" sorts first and is reported as the control group. To make placebo the control group instead:
> ```R
> library(dplyr)
> spark_df <- spark_df %>%
>   mutate(group = ifelse(group == "placebo", 0L, 1L))
> ```
> Placebo is now 0, which sorts before 1, so it is reported in the `.c` columns and the drug group in the `.t` columns.

## Differences from stddiff

On Spark data the results replicate `stddiff`, including its behaviour on unusual input, except:

- **Errors for one variable**: where `stddiff` stops with an error because of one variable, such as a singular matrix in `stddiff.category()`, the Spark version returns `NA` for that variable with a warning, and the other variables still get results. Local data frames are passed to `stddiff`, so there the error remains.
- **Exactly two groups**: the Spark version stops with an error unless the group column has exactly two values. `stddiff` doesn't check this: with more than two groups, `stddiff.binary()` and `stddiff.numeric()` silently compare the first two, and `stddiff.category()` stops with an unrelated error.
- **NaN counts as missing**: in every function, including `stddiff.binary()` and `stddiff.category()`, where `stddiff` keeps `NaN` as a value. sparklyr's `copy_to()` turns some R `NA`s into `NaN`, such as those from arithmetic like `(bmi >= 30) * 1`, and `NaN` is never a real category.
- **Large groups**: with more than about 46,000 rows per group, `stddiff`'s integer counts overflow and its confidence limits are `NA`, with the warning "NAs produced by integer overflow". The Spark version computes them. Local data frames are passed to `stddiff`, so they get `NA`.
- **Unusual number formats**: `stddiff.numeric()` converts strings as `as.numeric()` does for decimals, `Inf` and `NaN`, but unusual formats that `as.numeric()` also reads, such as hexadecimal (`"0x1A"`), become `NA` with its coercion warning.
- **Custom level order**: Spark has no factor type, so the data can't carry a custom level order. Groups and levels follow their sorted order, which matches `stddiff`'s default (`as.factor()`). On a local data frame, `stddiff` follows the level order of a factor column.

> [!Note]
> Like `stddiff`, `stddiff.binary()` doesn't check that variables are binary. With more than two distinct values the results aren't meaningful: the proportions can exceed 1, and the standardized difference can be `NaN`, with R's "NaNs produced" warning.


# Installation

## CRAN
```R
install.packages("stddiff.spark")
```

## GitHub
```R
# install.packages("remotes") # if you don’t have it
remotes::install_github("alicja-januszkiewicz/stddiff.spark")
```

# Usage
```R
library(sparklyr)
library(dplyr)
library(stddiff.spark)

# connect to Spark
sc <- spark_connect(master = "local")

# create example local data
my_data <- data.frame(
  treatment = c(1, 0, 1, 0, 1, 0),
  age       = c(34, 28, 45, 30, 50, 33),
  bmi       = c(22.1, 24.3, 27.8, 23.5, 28.2, 25.0),
  weight    = c(70, 65, 85, 68, 90, 72)
)

# copy data to Spark
spark_df <- copy_to(sc, my_data, overwrite = TRUE)

# compute standardized differences for numeric variables
stddiff.numeric(spark_df, gcol = 1, vcol = 2:4)

# disconnect Spark
spark_disconnect(sc)
```

# Requirements
- Apache Spark (tested with 3.4.4 and 4.2.0)
- sparklyr (>= 1.8.4). sparklyr versions before 1.9.5 need dbplyr below 2.6.0.
