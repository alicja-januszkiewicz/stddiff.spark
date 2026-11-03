# Parity checks go in the parity table below; only deliberate differences
# from stddiff and checks of the environment get tests of their own.

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

expect_matrices_equal <- function(actual, expected, tol = 1e-10, info = NULL) {
  testthat::expect_equal(dim(actual), dim(expected), info = info)
  testthat::expect_equal(rownames(actual), rownames(expected), info = info)
  testthat::expect_equal(colnames(actual), colnames(expected), info = info)

  finite <- is.finite(actual) & is.finite(expected)
  if (any(finite)) {
    testthat::expect_equal(
      actual[finite], expected[finite], tolerance = tol, info = info
    )
  }

  # NA, NaN and Inf must match too, not only the finite values
  testthat::expect_equal(is.na(actual), is.na(expected), info = info)
  testthat::expect_equal(is.nan(actual), is.nan(expected), info = info)
  testthat::expect_equal(is.infinite(actual), is.infinite(expected), info = info)
  testthat::expect_equal(
    actual[is.infinite(actual)], expected[is.infinite(actual)], info = info
  )
}

# Run a stddiff.spark function on Spark data and the stddiff function on local
# data, and check the results and R's own warnings match. `warning`, if given,
# is a warning both must give.
expect_matches_stddiff <- function(fn, ref, spark_df, local_df, gcol, vcol,
                                   warning = NULL, info = NULL) {
  res_warnings <- testthat::capture_warnings(res <- fn(spark_df, gcol, vcol))
  ref_warnings <- testthat::capture_warnings(ref <- ref(local_df, gcol, vcol))
  expect_matrices_equal(res, ref, info = info)
  if (!is.null(warning)) {
    testthat::expect_true(any(grepl(warning, ref_warnings)), info = info)
    testthat::expect_true(any(grepl(warning, res_warnings)), info = info)
  }
  for (msg in c("NaNs produced", "NAs introduced by coercion")) {
    testthat::expect_equal(
      sum(grepl(msg, res_warnings)),
      sum(grepl(msg, ref_warnings)),
      label = paste("number of", msg, "warnings"),
      info = info
    )
  }
  invisible(res)
}

# One package per call: with several, a missing one only gives a warning
testthat::skip_if_not_installed("sparklyr")
testthat::skip_if_not_installed("stddiff")
testthat::skip_on_cran()

spark_fns <- list(
  binary = stddiff.binary,
  category = stddiff.category,
  numeric = stddiff.numeric
)
stddiff_fns <- list(
  binary = stddiff::stddiff.binary,
  category = stddiff::stddiff.category,
  numeric = stddiff::stddiff.numeric
)

.sc <- sparklyr::spark_connect(master = "local", app_name = "stddiff-tests")
withr::defer({
  if (exists(".sc")) {
    try(sparklyr::spark_disconnect(.sc), silent = TRUE)
  }
})

testthat::test_that("the tests run on the Spark version CI asks for", {
  testthat::skip_if(Sys.getenv("SPARK") == "", "SPARK is not set")
  testthat::expect_equal(
    as.character(sparklyr::spark_version(.sc)),
    Sys.getenv("SPARK")
  )
})

# -------------------------------------------------------------------------
# Shared data
# -------------------------------------------------------------------------

# mtcars with missing values, in plain columns of every type the cases need
# (Spark has no factors)
cars <- local({
  d <- mtcars
  d$vs[c(1, 5, 10)] <- NA
  d$cyl[c(2, 8)] <- NA
  d$mpg[c(3, 7, 12)] <- NA
  d$hp[c(4, 9)] <- NA
  data.frame(
    group = ifelse(d$am == 0, "control", "treatment"),
    # The same groups, sorting the other way round
    flipped = ifelse(d$am == 0, "untreated", "treatment"),
    am = as.integer(d$am),
    vs = as.integer(d$vs),
    cyl = as.integer(d$cyl),
    gear = d$gear,
    mpg = d$mpg,
    hp = d$hp,
    wt = d$wt,
    vs_even = d$vs %% 2 == 0,
    # An NA from arithmetic, which copy_to() sends to Spark as NaN
    cyl_odd = d$cyl %% 2,
    cyl_double = d$cyl,
    am_double = as.double(d$am),
    gear_high = ifelse(d$gear > 3, "high", "low"),
    gear_chr = as.character(d$gear),
    # gear with 4 sorting first, so it becomes the reference level
    gear_label = unname(c("3" = "c", "4" = "a", "5" = "b")[as.character(d$gear)]),
    # NaN where hp is missing, NA where mpg is
    nan_mpg = ifelse(is.na(d$hp), NaN, d$mpg),
    # Values in one group only
    mpg_t = ifelse(d$am == 1, d$mpg, NA),
    wt_c = ifelse(d$am == 0, d$wt, NA),
    one = 1,
    none = NA_real_
  )
})

# Strings as they occur in practice
strings <- data.frame(
  group = rep(c("control", "treatment"), each = 8),
  # Decimals with whitespace and exponents, junk and "NA" (NA with a
  # warning), and blanks (NA without one)
  s1 = c("1.5", " 2 ", "abc", "NA", "", "3.25", "-0.5", "\t4\n",
         "n/a", "+.5", "2e1", "1,000", "  ", "5", "6.5", "1E-2"),
  # Infinities, and NaN in any case, which is missing: no warning
  s2 = c("Inf", "1", "2", "NaN", "3", "Infinity", "4", "5",
         "6", "-inf", "nan", "7", "NAN", "8", "-INFINITY", "10"),
  # Only numbers: no warning
  s3 = c("1", "2", "3", "4", "5", "6", "7", "8",
         "2", "3", "4", "5", "6", "7", "8", "9")
)

# Doubles equal to 15 significant digits share a factor level in stddiff:
# 0.1 + 0.2 and 0.3, and 1 and 1.000000000000001, but not 1.00000000000001
near_equal <- local({
  near <- 0.1 + 0.2
  data.frame(
    g = c(0.3, near, 0.3, near, 0.3, 0.3, 1, 1, 1, 1, 1, 1),
    b = c(near, 0.3, 1, 0.3, 1, near, 1, 0.3, 1, 1, near, 1),
    k = c(1, 1.000000000000001, 1.00000000000001, 2, near, 0.3,
          2, 1, 1.00000000000001, 0.3, 2, 1.000000000000001),
    inf = c(-Inf, 0, Inf, Inf, 0, -Inf, 0, Inf, 0, -Inf, Inf, 0),
    x = c(1.5, NA, 3, 4.5, 2, 3.5, 5, 6.25, 4, 7, 5.5, 6)
  )
})

# Values that sort differently as strings than as stddiff sorts them: "10" <
# "2", "-1" < "-2", and "Banana" < "apple" in the C locale
ordering <- local({
  i2 <- c(1, 2, 2, 1, 1, 1, 2, 2, 2, 1, 2, 2, 1, 2, 1, 1)
  i3 <- c(1, 2, 3, 2, 3, 3, 1, 2, 2, 1, 1, 3, 2, 1, 3, 3)
  i4 <- c(1, 2, 3, 4, 3, 3, 1, 2, 4, 1, 1, 3, 2, 4, 3, 1)
  data.frame(
    group = rep(c("control", "treatment"), each = 8),
    int2 = c(2L, 10L)[i2],
    dbl2 = c(-2, -1)[i2],
    chr2 = c("apple", "Banana")[i2],
    int3 = c(2L, 10L, 100L)[i3],
    dbl3 = c(-2, -1, 0.5)[i3],
    chr3 = c("apple", "Banana", "cherry")[i3],
    chr4 = c("a", "b", "c", "d")[i4]
  )
})

# Binary variables with one, two and three values
binary_edge <- data.frame(
  group = c("control", "control", "treatment", "treatment"),
  zeros = 0L,
  ones = 1L,
  missing = NA_integer_,
  x1 = c(0L, 1L, 0L, 1L),
  x2 = c(1L, 0L, 1L, 0L),
  x3 = c(0L, NA, 1L, NA),
  y1 = c(0L, 1L, 2L, 1L),
  y2 = c(1L, NA, 2L, 0L),
  y3 = c(NA, NA, 0L, 1L)
)

# Names that internal columns and summaries use or could use
clash_names <- c(
  "var", "x", "p", "n", "miss", "mean", "sd",
  "group", "value", "v1", "v2", "arm", "c", "t", "freq", "rank", "x_mapped",
  "var__", "x__", "group__", "rank__", "freq__"
)
value_pairs <- list(
  c("var", "x"), c("var__", "x__"), c("group", "value"), c("c", "t"),
  c("p", "n")
)

# Group columns with those names, and group columns with those values
clash_groups <- local({
  d <- data.frame(bin = cars$vs, cat = cars$cyl, num = cars$mpg)
  for (name in clash_names) {
    d[[name]] <- cars$group
  }
  for (pair in value_pairs) {
    d[[paste("values", pair[1], pair[2], sep = "_")]] <-
      ifelse(cars$group == "control", pair[1], pair[2])
  }
  d
})
clash_group_cols <- setdiff(names(clash_groups), c("bin", "cat", "num"))

# Variable columns with those names
clash_vars <- local({
  d <- data.frame(assignment = cars$group)
  for (name in clash_names) {
    d[[name]] <- cars$cyl
  }
  d
})

# Dates and whole-second date-times (collect() counts a fraction of a second
# twice; see "date-times keep their fractions of a second")
datetimes <- local({
  days <- as.Date(c("2023-12-31", "2024-01-31", "2024-02-29"))
  times <- as.POSIXct(
    c("2024-01-31 08:00:00", "2024-01-31 17:30:15", "2024-03-31 23:59:59"),
    tz = "UTC"
  )
  i2 <- c(1, 2, 2, 1, 1, NA, 2, 2, 1, 2, 2, 2, 1, 1, 2, 1)
  i3 <- c(1, 2, 3, NA, 3, 3, 1, 2, 2, 1, NA, 3, 2, 1, 3, 1)
  arm <- rep(1:2, each = 8)
  data.frame(
    group = rep(c("control", "treatment"), each = 8),
    d2 = days[i2],
    d3 = days[i3],
    t2 = times[i2],
    t3 = times[i3],
    day_group = days[arm],
    time_group = times[arm]
  )
})

# A data frame copied to Spark, compared with stddiff on the data frame
copied <- function(df, name) {
  list(spark = sparklyr::copy_to(.sc, df, name, overwrite = TRUE), local = df)
}

# A Spark table, compared with stddiff on what collect() gives
collected <- function(spark) {
  local <- as.data.frame(dplyr::collect(spark))
  # sparklyr 1.9's collect() gives NaN for a missing date-time, which counts
  # as missing
  local[] <- lapply(local, function(x) {
    if (inherits(x, "POSIXct")) x[is.nan(x)] <- NA
    x
  })
  list(spark = spark, local = local)
}

# Each data set is copied to Spark once
shared <- list(
  cars = copied(cars, "cars"),
  strings = copied(strings, "strings"),
  near_equal = copied(near_equal, "near_equal"),
  ordering = copied(ordering, "ordering"),
  binary_edge = copied(binary_edge, "binary_edge"),
  clash_groups = copied(clash_groups, "clash_groups"),
  clash_vars = copied(clash_vars, "clash_vars"),
  datetimes = collected(sparklyr::copy_to(.sc, datetimes, overwrite = TRUE))
)
# Types that only Spark makes: collect() gives decimals as numbers, and
# calendar intervals and NULL columns as strings
shared$spark_types <- collected(dplyr::mutate(
  shared$cars$spark,
  cyl_dec = dplyr::sql("CAST(cyl AS DECIMAL(10, 2))"),
  cal = dplyr::sql("make_interval(0, 0, 0, CAST(gear AS INT), 0, 0, 0)"),
  nul = dplyr::sql("NULL")
))

testthat::test_that("the shared data has the Spark types and values the cases need", {
  count_nan <- function(spark, col) {
    spark |>
      dplyr::filter(dplyr::sql(paste0("isnan(", col, ")"))) |>
      dplyr::count() |>
      dplyr::pull(n)
  }
  # NaN in Spark where R has an NA from arithmetic, or NaN
  testthat::expect_equal(count_nan(shared$cars$spark, "cyl_odd"), 2)
  testthat::expect_equal(count_nan(shared$cars$spark, "nan_mpg"), 2)

  # Spark keeps the near-equal values apart
  n_distinct <- shared$near_equal$spark |>
    dplyr::summarise(
      g = dplyr::n_distinct(g),
      b = dplyr::n_distinct(b),
      k = dplyr::n_distinct(k)
    ) |>
    dplyr::collect()
  testthat::expect_equal(unlist(n_distinct), c(g = 3, b = 3, k = 6))

  type_of <- function(spark, cols) {
    schema <- sparklyr::sdf_schema(spark)
    unname(vapply(schema[cols], `[[`, "", "type"))
  }
  testthat::expect_equal(
    type_of(shared$datetimes$spark, c("d2", "t2")),
    c("DateType", "TimestampType")
  )
  testthat::expect_equal(
    type_of(shared$spark_types$spark, c("cyl_dec", "cal", "nul")),
    c("DecimalType(10,2)", "CalendarIntervalType", "NullType")
  )
  testthat::expect_type(shared$spark_types$local$cal, "character")
})

# -------------------------------------------------------------------------
# Parity table
# -------------------------------------------------------------------------

# Each case runs every function in `fns` with every group column in `gcol`
# and every set of variable columns in `vcol` (column names in `data`), on
# Spark and in stddiff, and compares the results, row names and R's
# warnings. `warning` is a warning the case must give.
parity_cases <- list(
  list(
    name = "binary variables of every type, in any order, repeated",
    data = "cars", fns = "binary",
    # String groups both ways round, and a numeric group, which is also a
    # variable in some sets
    gcol = c("group", "flipped", "am"),
    vcol = list(
      c("vs", "am"),
      c("vs_even", "cyl_odd", "am_double", "gear_high"),
      c("vs", "am", "vs"),
      # A continuous variable, coded 0, 1, 2, ... as in stddiff
      c("vs", "mpg")
    )
  ),
  list(
    name = "categorical variables of every type, in any order, repeated",
    data = "cars", fns = "category",
    gcol = c("group", "flipped", "am"),
    vcol = list(
      c("cyl", "gear"),
      c("vs_even", "cyl", "cyl_double", "gear_chr"),
      c("cyl", "cyl"),
      # Another reference level
      c("gear", "gear_label")
    )
  ),
  list(
    name = "numeric variables of every type, in any order, repeated",
    data = "cars", fns = "numeric",
    gcol = c("group", "flipped", "am"),
    vcol = list(
      c("wt", "mpg", "hp", "cyl"),
      c("mpg", "mpg"),
      c("mpg", "hp", "mpg"),
      # Logicals coded 0/1, and zero variance
      c("vs_even", "one"),
      # A group with no values: stddiff reports the other in the .c columns
      c("mpg_t", "wt_c"),
      # NaN is missing
      c("nan_mpg", "mpg"),
      # With gcol "am", the group column: its means are the factor codes 1
      # and 2, and later variables' missing counts are 0
      c("mpg", "am"),
      c("am", "mpg")
    )
  ),
  list(
    name = "numeric: a string group column as a variable",
    data = "cars", fns = "numeric",
    gcol = "group", vcol = list(c("mpg", "group"))
  ),
  list(
    name = "numeric: strings, with the coercion warning once per column",
    data = "strings", fns = "numeric",
    gcol = "group",
    vcol = list(c("s1", "s2", "s3", "s1"), "s1", "s2", "s3")
  ),
  list(
    name = "binary: near-equal doubles share a group and a value",
    data = "near_equal", fns = "binary", gcol = "g", vcol = list("b")
  ),
  list(
    name = "categorical: near-equal doubles share a level; infinities",
    data = "near_equal", fns = "category", gcol = "g",
    vcol = list(c("k", "inf"))
  ),
  list(
    name = "numeric: near-equal group values share a group",
    data = "near_equal", fns = "numeric", gcol = "g", vcol = list(c("x", "g"))
  ),
  list(
    name = "binary: two values in stddiff's order",
    data = "ordering", fns = "binary", gcol = "group",
    vcol = list(c("int2", "dbl2", "chr2"))
  ),
  list(
    name = "binary: three or more values, with NaNs as in stddiff",
    data = "ordering", fns = "binary", gcol = "group",
    vcol = list(c("int3", "dbl3", "chr3", "chr4")),
    warning = "NaNs produced"
  ),
  list(
    name = "categorical: levels in stddiff's order",
    data = "ordering", fns = "category", gcol = "group",
    vcol = list(c("int3", "dbl3", "chr3"))
  ),
  list(
    name = "binary: one value, no values, and two values",
    data = "binary_edge", fns = "binary", gcol = "group",
    vcol = list(c("zeros", "ones", "missing"), c("x1", "x2", "x3"))
  ),
  list(
    name = "binary: three values, with NaNs as in stddiff",
    data = "binary_edge", fns = "binary", gcol = "group",
    vcol = list(c("y1", "y2", "y3")),
    warning = "NaNs produced"
  ),
  list(
    name = "binary: group columns named or valued like internal columns",
    data = "clash_groups", fns = "binary",
    gcol = clash_group_cols, vcol = list("bin")
  ),
  list(
    name = "categorical: group columns named or valued like internal columns",
    data = "clash_groups", fns = "category",
    gcol = clash_group_cols, vcol = list("cat")
  ),
  list(
    name = "numeric: group columns named or valued like internal columns",
    data = "clash_groups", fns = "numeric",
    gcol = clash_group_cols, vcol = list("num")
  ),
  list(
    name = "variable columns named like internal columns",
    data = "clash_vars", fns = c("binary", "category", "numeric"),
    gcol = "assignment", vcol = list(clash_names)
  ),
  list(
    name = "binary: dates and date-times, also as the group",
    data = "datetimes", fns = "binary",
    gcol = c("group", "day_group", "time_group"), vcol = list(c("d2", "t2"))
  ),
  list(
    name = "categorical: dates and date-times, also as the group",
    data = "datetimes", fns = "category",
    gcol = c("group", "day_group", "time_group"), vcol = list(c("d3", "t3"))
  ),
  list(
    name = "numeric: dates and date-times as days and seconds",
    data = "datetimes", fns = "numeric",
    gcol = c("group", "day_group", "time_group"),
    vcol = list(c("d2", "d3", "t2", "t3"))
  ),
  list(
    name = "categorical: decimals and calendar intervals",
    data = "spark_types", fns = "category",
    gcol = "group", vcol = list(c("cyl_dec", "cal"))
  ),
  list(
    name = "binary: calendar intervals and NULL columns",
    data = "spark_types", fns = "binary",
    gcol = "group", vcol = list(c("cal", "nul"))
  )
)

run_case <- function(case) {
  d <- shared[[case$data]]
  cols <- colnames(d$local)
  testthat::expect_equal(colnames(d$spark), cols)
  for (fn in case$fns) {
    for (g in case$gcol) {
      for (v in case$vcol) {
        expect_matches_stddiff(
          spark_fns[[fn]], stddiff_fns[[fn]], d$spark, d$local,
          match(g, cols), match(v, cols),
          warning = case$warning,
          info = paste0(
            case$name, ": ", fn, ", gcol ", g,
            ", vcol ", paste(v, collapse = " ")
          )
        )
      }
    }
  }
}

for (case in parity_cases) {
  testthat::test_that(paste("parity:", case$name), {
    run_case(case)
  })
}

# -------------------------------------------------------------------------
# Local data
# -------------------------------------------------------------------------

testthat::test_that("local data frames go to stddiff; verbose reports progress", {
  local <- shared$cars$local
  vcol <- list(binary = c(4, 3), category = c(5, 6), numeric = c(7, 8))
  for (fn in names(spark_fns)) {
    testthat::expect_identical(
      spark_fns[[fn]](local, 1, vcol[[fn]]),
      stddiff_fns[[fn]](local, 1, vcol[[fn]])
    )
    testthat::expect_message(
      spark_fns[[fn]](local, 1, vcol[[fn]], verbose = TRUE),
      "Using stddiff package"
    )
    messages <- testthat::capture_messages(
      spark_fns[[fn]](shared$cars$spark, 1, vcol[[fn]], verbose = TRUE)
    )
    testthat::expect_match(messages, "Processing 2", all = FALSE)
    testthat::expect_match(messages, "Complete!", all = FALSE)
  }
})

# -------------------------------------------------------------------------
# Wrong calls stop
# -------------------------------------------------------------------------

testthat::test_that("invalid indices error cleanly", {
  spark <- shared$cars$spark
  out <- length(colnames(spark)) + 1
  testthat::expect_error(
    stddiff.binary(spark, out, 1),
    paste0("gcol index ", out, " is out of range")
  )
  testthat::expect_error(
    stddiff.binary(spark, 0, 1),
    "gcol index 0 is out of range"
  )
  testthat::expect_error(
    stddiff.binary(spark, 1, c(2, out)),
    paste0("vcol index\\(es\\) out of range: ", out, "$")
  )
  testthat::expect_error(
    stddiff.binary(spark, 1, numeric()),
    "vcol must contain at least one column index"
  )
  testthat::expect_error(
    stddiff.binary(spark, "group", 4),
    "gcol must be a single numeric index"
  )
  testthat::expect_error(
    stddiff.binary(spark, 1, "vs"),
    "vcol must be numeric indices"
  )
})

testthat::test_that("column types stddiff can't process stop with an error", {
  # Values that vary, as in real columns
  day <- "CAST(gear AS INT)"
  rejected <- c(
    arr = "array(gear, 1)",
    map = "map('k', gear)",
    struct = "named_struct('x', gear)",
    bin = "CAST(CAST(gear AS STRING) AS BINARY)",
    ntz = paste0(
      "CAST(make_timestamp(2024, 1, ", day, ", 12, 0, 0) AS TIMESTAMP_NTZ)"
    ),
    days = paste0("make_dt_interval(", day, ", 0, 0, 0)"),
    months = paste0("make_ym_interval(1, ", day, ")")
  )
  if (sparklyr::spark_version(.sc) >= "4.0.0") {
    rejected <- c(rejected, variant = "parse_json(CAST(gear AS STRING))")
  }
  # sparklyr 1.8 warns "out-of-range values treated as 0 in coercion to raw"
  # when dplyr meets a binary column; nothing else may warn
  warnings <- testthat::capture_warnings({
    odd <- dplyr::mutate(shared$cars$spark, !!!lapply(rejected, dplyr::sql))
    local <- as.data.frame(dplyr::collect(odd))
  })
  testthat::expect_true(all(grepl("coercion to raw", warnings)))
  cols <- colnames(odd)
  for (col in names(rejected)) {
    vcol <- match(col, cols)
    for (fn in names(spark_fns)) {
      info <- paste(col, fn)
      # collect() gives lists or Java references, which stddiff can't process
      testthat::expect_error(stddiff_fns[[fn]](local, 1, vcol), info = info)
      testthat::expect_error(
        spark_fns[[fn]](odd, 1, vcol),
        paste0("Column '", col, "' is [A-Za-z]+ but must be"),
        info = info
      )
    }
  }
  testthat::expect_error(
    stddiff.binary(odd, match("arr", cols), 2),
    "Group column 'arr' is ArrayType"
  )
})

testthat::test_that("the group column must have exactly two values", {
  df <- data.frame(
    g = rep(c("a", "b", "c"), each = 4),
    x = c(1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6),
    b = c(0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 1)
  )
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  # stddiff.binary() and stddiff.numeric() silently compare the first two
  # groups; stddiff.category() stops with an unrelated error
  ref <- stddiff::stddiff.numeric(df, 1, 2)
  testthat::expect_equal(unname(ref[1, c("mean.c", "mean.t")]), c(2.5, 3.5))
  ref <- stddiff::stddiff.binary(df, 1, 3)
  testthat::expect_equal(unname(ref[1, c("p.c", "p.t")]), c(0.5, 0.75))
  testthat::expect_error(stddiff::stddiff.category(df, 1, 3))

  for (fn in spark_fns) {
    testthat::expect_error(fn(spark_df, 1, 3), "exactly 2 levels")
  }

  one <- sparklyr::copy_to(.sc, df[df$g == "a", ], overwrite = TRUE)
  for (fn in spark_fns) {
    testthat::expect_error(fn(one, 1, 3), "exactly 2 levels")
  }
})

# -------------------------------------------------------------------------
# Deliberate differences from stddiff
# -------------------------------------------------------------------------

testthat::test_that("NA with a warning where stddiff stops for a variable", {
  # stddiff stops on the `bad` variables; the Spark version gives them NA
  # with a warning naming them and the `reason`, and the other variables
  # what stddiff gives them alone
  expect_na_for <- function(fn, data, gcol, vcol, bad, reason) {
    d <- shared[[data]]
    cols <- colnames(d$local)
    g <- match(gcol, cols)
    info <- paste(fn, paste(vcol, collapse = " "))
    testthat::capture_warnings(testthat::expect_error(
      stddiff_fns[[fn]](d$local, g, match(vcol, cols)),
      info = info
    ))

    warnings <- testthat::capture_warnings(
      res <- spark_fns[[fn]](d$spark, g, match(vcol, cols))
    )
    for (b in bad) {
      testthat::expect_match(
        warnings, paste0("variable '", b, "' \\(", reason),
        all = FALSE, info = info
      )
    }
    ref <- stddiff_fns[[fn]](d$local, g, match(setdiff(vcol, bad), cols))
    expect_matrices_equal(res[rownames(ref), , drop = FALSE], ref, info = info)
    bad_rows <- setdiff(rownames(res), rownames(ref))
    testthat::expect_true(
      all(is.na(res[bad_rows, c("stddiff", "stddiff.l", "stddiff.u")])),
      info = info
    )
    invisible(list(res = res, warnings = warnings))
  }

  # No values
  expect_na_for("numeric", "cars", "group", c("mpg", "none"), "none",
                "no rows to aggregate")
  out <- expect_na_for("category", "cars", "group", c("none", "cyl"), "none",
                       "no non-missing values")
  # One row named after the variable, with its missing counts
  testthat::expect_equal(
    unname(out$res["none", c("missing.c", "missing.t")]),
    as.vector(table(cars$group))
  )
  expect_na_for("category", "spark_types", "group", c("nul", "cyl"), "nul",
                "no non-missing values")
  # Calendar intervals, which as.numeric() turns into NA with a warning
  out <- expect_na_for("numeric", "spark_types", "group",
                       c("cal", "nul", "mpg"), c("cal", "nul"),
                       "no rows to aggregate")
  testthat::expect_match(out$warnings, "NAs introduced by coercion", all = FALSE)

  # One level, and a singular matrix (the group column as a variable)
  expect_na_for("category", "cars", "group", c("one", "cyl"), "one",
                "only one level")
  expect_na_for("category", "cars", "am", c("vs", "am"), "am", ".*singular")
})

testthat::test_that("NaN from copy_to() counts as missing, like R's NA", {
  # Indicators and bands derived by arithmetic, whose NAs copy_to() sends to
  # Spark as NaN; in R they are NA, which stddiff counts as missing
  bmi <- c(24, NA, 31, 28, 35, 22, NA, 30, 27, 33, 29, 26, 32, NA, 21, 36)
  age <- c(45, 61, NA, 52, 38, 70, 49, 55, 63, NA, 41, 58, 47, 66, 53, 44)
  df <- data.frame(
    group = rep(c("control", "treatment"), each = 8),
    obese = (bmi >= 30) * 1,
    bmi_band = floor(bmi / 5) * 5,
    older = (age > 50) * 1
  )
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  # Spark has NaN where R has NA
  count_nan <- function(col) {
    spark_df |>
      dplyr::filter(dplyr::sql(paste0("isnan(", col, ")"))) |>
      dplyr::count() |>
      dplyr::pull(n)
  }
  testthat::expect_equal(
    vapply(c("obese", "bmi_band", "older"), count_nan, numeric(1)),
    c(obese = 3, bmi_band = 3, older = 2)
  )

  # A binary indicator and a categorical variable, and as the group column,
  # whose NaN rows belong to neither group
  for (gcol in c(1, 4)) {
    expect_matches_stddiff(
      stddiff.binary, stddiff::stddiff.binary, spark_df, df, gcol, 2
    )
    expect_matches_stddiff(
      stddiff.category, stddiff::stddiff.category, spark_df, df, gcol, 3
    )
  }
  expect_matches_stddiff(
    stddiff.numeric, stddiff::stddiff.numeric, spark_df, df, 4, 3
  )
})

testthat::test_that("unusual number formats in strings are NA with a warning", {
  df <- data.frame(
    group = rep(c("control", "treatment"), each = 3),
    s = c("0x1A", "1", "2", "3", "4", "-nan")
  )
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  # as.numeric() reads them: hexadecimal, and NaN with a sign
  testthat::expect_identical(as.numeric(df$s[c(1, 6)]), c(26, NaN))
  testthat::expect_warning(
    res <- stddiff.numeric(spark_df, 1, 2),
    "NAs introduced by coercion"
  )
  testthat::expect_equal(unname(res[1, c("missing.c", "missing.t")]), c(1, 1))
})

testthat::test_that("large groups get the limits stddiff's integers overflow on", {
  # 47,000 rows per group: stddiff's n[1] * n[2] from table() passes 2^31 - 1
  n <- 47000
  df <- data.frame(
    group = rep(c("control", "treatment"), each = n),
    b = c(rep(c(0, 1), length.out = n), rep(c(0, 1, 1), length.out = n)),
    k = c(rep(c("a", "b", "c"), length.out = n),
          rep(c("a", "b", "b", "c"), length.out = n)),
    x = c(seq_len(n) %% 7, seq_len(n) %% 9 + 0.5)
  )
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  # stddiff's own code, with table()'s counts as doubles so they can't overflow
  in_doubles <- function(fn) {
    env <- new.env(parent = environment(fn))
    env$table <- function(...) {
      counts <- base::table(...)
      storage.mode(counts) <- "double"
      counts
    }
    environment(fn) <- env
    fn
  }

  limits <- c("stddiff.l", "stddiff.u")
  vcol <- list(binary = 2, category = 3, numeric = 4)
  for (fn in names(spark_fns)) {
    testthat::expect_warning(
      ref <- stddiff_fns[[fn]](df, 1, vcol[[fn]]),
      "NAs produced by integer overflow"
    )
    testthat::expect_true(all(is.na(ref[1, limits])))

    res <- spark_fns[[fn]](spark_df, 1, vcol[[fn]])
    testthat::expect_equal(res[, "stddiff"], ref[, "stddiff"])
    testthat::expect_true(all(is.finite(res[1, limits])))
    expect_matrices_equal(res, in_doubles(stddiff_fns[[fn]])(df, 1, vcol[[fn]]))
  }
})

testthat::test_that("date-times keep their fractions of a second", {
  # copy_to() drops fractions of a second and collect() counts them twice, so
  # the date-times are made in Spark, and the same values in R
  seconds <- data.frame(
    group = rep(c("control", "treatment"), each = 6),
    sec = c(0.25, 0.5, 1.75, 0.25, 0.5, 0.5, 1.75, 1.75, 0.25, 0.5, 1.75, 1.75),
    group_sec = rep(c(0.25, 0.5), each = 6)
  )
  at <- function(col) {
    dplyr::sql(paste0(
      "make_timestamp(2024, 1, 31, 8, 0, CAST(", col, " AS DECIMAL(8, 6)), 'UTC')"
    ))
  }
  spark_df <- sparklyr::copy_to(.sc, seconds, overwrite = TRUE) |>
    dplyr::mutate(t = !!at("sec"), time_group = !!at("group_sec")) |>
    dplyr::select(group, t, time_group)
  # In R's time zone, as the Spark version builds them
  start <- as.numeric(as.POSIXct("2024-01-31 08:00:00", tz = "UTC"))
  local <- data.frame(
    group = seconds$group,
    t = .POSIXct(start + seconds$sec),
    time_group = .POSIXct(start + seconds$group_sec)
  )

  # Row names show the fractions: "t 2024-01-31 08:00:00.25"
  expect_matches_stddiff(
    stddiff.category, stddiff::stddiff.category, spark_df, local, 1, 2
  )
  expect_matches_stddiff(
    stddiff.numeric, stddiff::stddiff.numeric, spark_df, local, 1, 2
  )
  expect_matches_stddiff(
    stddiff.numeric, stddiff::stddiff.numeric, spark_df, local, 3, 2
  )
})

# -------------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------------

testthat::test_that("the parity table holds with Spark's ANSI mode on", {
  # On by default from Spark 4; restore whatever it was
  conf <- sparklyr::invoke(sparklyr::spark_session(.sc), "conf")
  ansi <- sparklyr::invoke(conf, "get", "spark.sql.ansi.enabled")
  sparklyr::invoke(conf, "set", "spark.sql.ansi.enabled", "true")
  withr::defer(sparklyr::invoke(conf, "set", "spark.sql.ansi.enabled", ansi))

  # In ANSI mode a cast errors on strings that aren't numbers
  testthat::expect_error(
    dplyr::collect(dplyr::mutate(shared$strings$spark, x = as.numeric(s1))),
    "CAST_INVALID_INPUT"
  )

  # Where ANSI mode was already on, the parity tests above ran with it
  testthat::skip_if(identical(ansi, "true"), "ANSI mode was already on")
  for (case in parity_cases) {
    run_case(case)
  }
})
