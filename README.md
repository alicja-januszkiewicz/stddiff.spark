[![CRAN status](https://www.r-pkg.org/badges/version/stddiff.spark)](
https://CRAN.R-project.org/package=stddiff.spark
)

# Introduction

`stddiff.spark` provides Spark-compatible implementations of the standardized difference calculations from the `stddiff` package. The interface is identical to `stddiff`, so you can swap your existing calls in-place without changing your workflow.  

Because Spark DataFrames do not have native factor types, categorical variables are encoded using alphabetic ordering: the first level alphabetically becomes 0, the second becomes 1, and so on. This ensures consistent, deterministic calculations for binary and multi-level categorical variables.

> [!Note]
> If you want to choose a specific reference category, you must update the values in your Spark DataFrame so that the desired reference level comes first alphabetically. For example:
> ```R
> library(dplyr)
> # Suppose original category: "Control", "Treatment"
> spark_df <- spark_df %>%
>   mutate(group = ifelse(group == "Treatment", "A_Treatment", group))
> ```
> Here, prefixing "Treatment" with "A_" ensures it comes first alphabetically, making it the reference level for standardized difference calculations.

Functions automatically dispatch to the `stddiff` package when non-Spark data is supplied, so the same code works seamlessly on both local R data frames and Spark DataFrames.


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
- Apache Spark (tested with 3.4.4)
- sparklyr (>= 1.8.0). 
