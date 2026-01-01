# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

get_col_index <- function(data, cols) {
  match(cols, colnames(data))
}

expect_matrices_equal <- function(actual, expected, tol = 1e-10) {
  testthat::expect_equal(dim(actual), dim(expected))
  testthat::expect_equal(rownames(actual), rownames(expected))
  testthat::expect_equal(colnames(actual), colnames(expected))

  finite <- is.finite(actual) & is.finite(expected)
  if (any(finite)) {
    testthat::expect_equal(actual[finite], expected[finite], tolerance = tol)
  }

  testthat::expect_equal(is.na(actual), is.na(expected))
}

make_test_data <- function() {
  set.seed(123)

  mtcars |>
    dplyr::mutate(
      vs = dplyr::if_else(dplyr::row_number() %in% c(1, 5, 10), NA, vs),
      cyl = dplyr::if_else(dplyr::row_number() %in% c(2, 8), NA, cyl),
      mpg = dplyr::if_else(dplyr::row_number() %in% c(3, 7, 12), NA_real_, mpg),
      hp = dplyr::if_else(dplyr::row_number() %in% c(4, 9), NA_real_, hp)
    ) |>
    dplyr::mutate(
      am_factor = factor(am, labels = c("automatic", "manual")),
      vs_factor = factor(vs, labels = c("V-shaped", "straight")),
      cyl_factor = factor(cyl),
      gear_factor = factor(gear),
      mpg_num = mpg,
      hp_num = hp,
      wt_num = wt,
      group = factor(ifelse(am == 0, "control", "treatment"))
    )
}

# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------

testthat::skip_if_not_installed(c("sparklyr", "stddiff"))
testthat::skip_on_cran()

.local_data <- make_test_data()
.sc <- sparklyr::spark_connect(master = "local", app_name = "stddiff-tests")
.spark_data <- sparklyr::copy_to(.sc, .local_data, overwrite = TRUE) |>
  dplyr::mutate(
    am_factor = as.integer(am),
    vs_factor = as.integer(vs),
    cyl_factor = as.integer(cyl),
    gear_factor = as.integer(gear),
    group = as.character(ifelse(am == 0, "control", "treatment"))
  )

withr::defer({
  if (exists(".sc")) {
    try(sparklyr::spark_disconnect(.sc), silent = TRUE)
  }
})

# -------------------------------------------------------------------------
# Input validation
# -------------------------------------------------------------------------

testthat::test_that("invalid indices error cleanly", {
  suppressWarnings({
    testthat::expect_error(stddiff.binary(.spark_data, 1, 2))
    testthat::expect_error(stddiff.binary(.spark_data, 1, numeric()))
    testthat::expect_error(stddiff.binary(.spark_data, 999, 1))
  })
})

testthat::test_that("non-binary grouping errors", {
  bad <- .local_data |>
    dplyr::mutate(group3 = factor(sample(c("A", "B", "C"), dplyr::n(), TRUE)))
  spark_bad <- sparklyr::copy_to(.sc, bad, overwrite = TRUE)

  gcol <- get_col_index(spark_bad, "group3")
  vcol <- get_col_index(spark_bad, "am_factor")
  testthat::expect_error(stddiff.binary(spark_bad, gcol, vcol))
})

# -------------------------------------------------------------------------
# Dispatch
# -------------------------------------------------------------------------

testthat::test_that("data.frame dispatch matches stddiff", {
  gcol <- get_col_index(.local_data, "group")
  vcol <- get_col_index(.local_data, c("am_factor", "vs_factor"))

  res <- stddiff.binary(.local_data, gcol, vcol)
  ref <- stddiff::stddiff.binary(.local_data, gcol, vcol)
  expect_matrices_equal(res, ref)
})

testthat::test_that("spark dispatch returns matrix", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("am_factor", "vs_factor"))

  res <- stddiff.binary(.spark_data, gcol, vcol)
  testthat::expect_true(is.matrix(res))
})

# -------------------------------------------------------------------------
# Binary variables
# -------------------------------------------------------------------------

testthat::test_that("binary results match reference", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("am_factor", "vs_factor"))

  spark <- stddiff.binary(.spark_data, gcol, vcol)
  ref <- stddiff::stddiff.binary(.local_data, gcol, vcol)
  expect_matrices_equal(spark, ref)
})

testthat::test_that("binary preserves variable order", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("vs_factor", "am_factor"))

  res <- stddiff.binary(.spark_data, gcol, vcol)
  testthat::expect_equal(rownames(res), c("vs_factor", "am_factor"))
})

# -------------------------------------------------------------------------
# Categorical variables
# -------------------------------------------------------------------------

testthat::test_that("categorical matches reference", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("cyl_factor", "gear_factor"))

  spark <- stddiff.category(.spark_data, gcol, vcol)
  ref <- stddiff::stddiff.category(.local_data, gcol, vcol)
  expect_matrices_equal(spark, ref)
})

# -------------------------------------------------------------------------
# Numeric variables
# -------------------------------------------------------------------------

testthat::test_that("numeric matches reference", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("mpg_num", "hp_num", "wt_num"))

  spark <- stddiff.numeric(.spark_data, gcol, vcol)
  ref <- stddiff::stddiff.numeric(.local_data, gcol, vcol)
  expect_matrices_equal(spark, ref)
})

testthat::test_that("numeric preserves order", {
  gcol <- get_col_index(.spark_data, "group")
  vcol <- get_col_index(.spark_data, c("wt_num", "mpg_num", "hp_num"))

  res <- stddiff.numeric(.spark_data, gcol, vcol)
  testthat::expect_equal(rownames(res), c("wt_num", "mpg_num", "hp_num"))
})

# -------------------------------------------------------------------------
# Edge cases
# -------------------------------------------------------------------------

testthat::test_that("handles all-NA variable", {
  df <- .local_data |> dplyr::mutate(all_na = NA_real_)
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  gcol <- get_col_index(spark_df, "group")
  vcol <- get_col_index(spark_df, "all_na")
  res <- stddiff.numeric(spark_df, gcol, vcol)
  testthat::expect_true(is.matrix(res))
})

testthat::test_that("handles zero variance", {
  df <- data.frame(
    group = factor(rep(c("control", "treatment"), each = 10)),
    x = 1
  )
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  gcol <- get_col_index(spark_df, "group")
  vcol <- get_col_index(spark_df, "x")
  res <- stddiff.numeric(spark_df, gcol, vcol)
  testthat::expect_true(is.nan(res[1, "stddiff"]) || res[1, "stddiff"] == 0)
})

testthat::test_that("categorical works when gcol has numeric values 1/2", {
  # Make a simple categorical dataset
  df <- data.frame(
    age_group = factor(c("16-19", "20-24", "25-29", "16-19", "20-24", "25-29")),
    group = c(1, 1, 1, 2, 2, 2) # numeric group
  )

  # Copy to Spark
  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE) |>
    dplyr::mutate(group = as.integer(group)) # numeric group

  # Use gcol as index
  gcol <- get_col_index(spark_df, "group")
  vcol <- get_col_index(spark_df, "age_group")

  # Run stddiff.category
  res <- stddiff.category(spark_df, gcol, vcol, verbose = TRUE)

  # Check results are numeric matrix with expected rownames
  testthat::expect_true(is.matrix(res))
  testthat::expect_equal(
    rownames(res),
    paste("age_group", levels(df$age_group))
  )

  # Check that stddiff column exists and is numeric
  testthat::expect_true("stddiff" %in% colnames(res))
  testthat::expect_true(is.numeric(res[, "stddiff"]))
})

testthat::test_that("stddiff.binary matches reference for mixed types", {
  df <- .local_data |>
    dplyr::mutate(
      logical_bin = vs %% 2 == 0,
      int_bin     = cyl %% 2,
      double_bin  = as.double(am),
      char_bin    = ifelse(gear > 3, "high", "low")
    )

  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  gcol <- get_col_index(df, "group")
  vcol <- get_col_index(df, c("logical_bin", "int_bin", "double_bin", "char_bin"))

  ref <- stddiff::stddiff.binary(df, gcol, vcol)
  res <- suppressWarnings(stddiff.binary(spark_df, gcol, vcol))

  expect_matrices_equal(res, ref)
})

testthat::test_that("stddiff.category matches reference for mixed types", {
  df <- .local_data |>
    dplyr::mutate(
      logical_cat = vs %% 2 == 0,
      int_cat     = cyl,
      double_cat  = as.double(cyl),
      char_cat    = as.character(gear)
    )

  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  gcol <- get_col_index(df, "group")
  vcol <- get_col_index(df, c("logical_cat", "int_cat", "double_cat", "char_cat"))

  ref <- stddiff::stddiff.category(df, gcol, vcol)
  res <- suppressWarnings(stddiff.category(spark_df, gcol, vcol))

  expect_matrices_equal(res, ref)
})

testthat::test_that("stddiff.numeric matches reference for mixed types", {
  df <- .local_data |>
    dplyr::mutate(
      int_num    = cyl,
      double_num = as.double(hp)
    )

  spark_df <- sparklyr::copy_to(.sc, df, overwrite = TRUE)

  gcol <- get_col_index(df, "group")
  vcol <- get_col_index(df, c("int_num", "double_num"))

  ref <- stddiff::stddiff.numeric(df, gcol, vcol)
  res <- stddiff.numeric(spark_df, gcol, vcol)

  expect_matrices_equal(res, ref)
})
