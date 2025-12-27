# stddiff.R - Spark implementations of standardized difference calculations
#
# These functions provide Spark-compatible implementations of standardized
# difference calculations from the stddiff package. They are functionally
# equivalent to the base R versions but operate on Spark DataFrames.

# Helper Functions --------------------------------------------------------

#' Validate inputs for stddiff functions
#' @keywords internal
validate_stddiff_inputs <- function(data, gcol, vcol, type) {
  # 1. Argument and Class Checks
  if (!type %in% c("binary", "category", "numeric")) {
    stop(
      "Invalid type: '",
      type,
      "'. Must be 'binary', 'category', or 'numeric'.",
      call. = FALSE
    )
  }

  if (!inherits(data, "tbl_spark")) {
    stop("data must be a Spark DataFrame (tbl_spark).", call. = FALSE)
  }

  # 2. Schema Retrieval
  schema <- sparklyr::sdf_schema(data)
  all_cols <- names(schema)
  n_cols <- length(all_cols)

  # 3. Check gcol
  if (length(gcol) != 1 || !is.numeric(gcol)) {
    stop("gcol must be a single numeric index", call. = FALSE)
  }
  if (gcol < 1 || gcol > n_cols) {
    stop(
      "gcol index ",
      gcol,
      " is out of range (1-",
      n_cols,
      ")",
      call. = FALSE
    )
  }

  # 4. Check vcol
  if (length(vcol) == 0) {
    stop("vcol must contain at least one column index", call. = FALSE)
  }
  if (!is.numeric(vcol)) {
    stop("vcol must be numeric indices", call. = FALSE)
  }
  if (any(vcol < 1 | vcol > n_cols)) {
    bad_idx <- vcol[vcol < 1 | vcol > n_cols]
    stop(
      "vcol index(es) out of range: ",
      paste(bad_idx, collapse = ", "),
      call. = FALSE
    )
  }

  # 5. Type Definitions
  numeric_types <- c(
    "DoubleType",
    "IntegerType",
    "LongType",
    "FloatType",
    "DecimalType",
    "ShortType",
    "ByteType"
  )
  categorical_types <- c(
    "StringType",
    "IntegerType",
    "LongType",
    "BooleanType",
    "ByteType",
    "ShortType"
  )

  # 6. Group Column Safety Check
  gcol_name <- all_cols[[gcol]]
  gcol_type <- schema[[gcol_name]]$type
  if (gcol_type %in% c("DoubleType", "FloatType")) {
    warning(
      "Group column '",
      gcol_name,
      "' is ",
      gcol_type,
      ". Ensure it contains exactly two discrete values (e.g., 0 and 1) 
      to avoid precision errors.",
      call. = FALSE
    )
  } else if (!gcol_type %in% categorical_types) {
    stop(
      "Group column '",
      gcol_name,
      "' is ",
      gcol_type,
      " but must be a discrete type (String, Integer, or Boolean).",
      call. = FALSE
    )
  }

  # 7. Variable Type Validation Loop
  for (col in vcol) {
    col_type <- schema[[col]]$type

    if (type == "numeric") {
      if (!col_type %in% numeric_types) {
        stop(
          "Column '",
          col,
          "' is ",
          col_type,
          " but must be numeric for stddiff.numeric.",
          call. = FALSE
        )
      }
    } else {
      # Logic for binary and category
      if (col_type %in% categorical_types) {
        next
      } else if (col_type %in% c("DoubleType", "FloatType", "DecimalType")) {
        warning(
          "Column '",
          col,
          "' is ",
          col_type,
          ". Treating as discrete. ",
          "Ensure it contains no fractional values.",
          call. = FALSE
        )
      } else {
        stop(
          "Column '",
          col,
          "' is ",
          col_type,
          " but must be a discrete type (String, Int, Boolean) for ",
          type,
          " calculations.",
          call. = FALSE
        )
      }
    }
  }

  invisible(TRUE)
}

#' Check if group variable has exactly 2 levels
#' @keywords internal
check_binary_group <- function(group_levels, gcol_name) {
  if (length(group_levels) != 2) {
    stop(
      "Group variable '",
      gcol_name,
      "' must have exactly 2 levels. Found ",
      length(group_levels),
      ": ",
      paste(group_levels, collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}


# Main Functions ----------------------------------------------------------

#' @keywords internal
stddiff_binary_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol, type = "binary")

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  vcol_names <- all_cols[vcol]

  if (verbose) {
    message("Processing ", length(vcol), " binary variable(s)...")
  }

  # Pivot to long format
  long <- data |>
    dplyr::select(dplyr::all_of(c(gcol, vcol))) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(vcol_names),
      names_to = "var",
      values_to = "x"
    )

  # Aggregate and collect
  stats <- long |>
    dplyr::group_by(var) |>
    dplyr::mutate(
      # Get the "low" and "high" values per variable (lexicographic ordering)
      min_val = min(x, na.rm = TRUE),
      max_val = max(x, na.rm = TRUE)
    ) |>
    dplyr::mutate(
      x_mapped = dplyr::case_when(
        x == min_val ~ 0L,
        x == max_val ~ 1L,
        TRUE ~ NA_integer_
      )
    ) |>
    dplyr::group_by(var, .data[[gcol_name]]) |>
    dplyr::summarise(
      p = mean(x_mapped, na.rm = TRUE),
      n = sum(dplyr::if_else(is.na(x_mapped), 0L, 1L), na.rm = TRUE),
      miss = sum(dplyr::if_else(is.na(x_mapped), 1L, 0L), na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::collect()

  # Get group levels (sorted)
  group_levels <- sort(unique(stats[[gcol_name]]))
  check_binary_group(group_levels, gcol_name)

  # Pivot to wide format and compute statistics
  stats_wide <- stats |>
    dplyr::arrange(var, .data[[gcol_name]]) |>
    tidyr::pivot_wider(
      id_cols = var,
      names_from = dplyr::all_of(gcol_name),
      values_from = c(p, n, miss)
    )

  tryCatch(
    {
      result <- stats_wide |>
        dplyr::rename(
          p.c = !!paste0("p_", group_levels[1]),
          p.t = !!paste0("p_", group_levels[2]),
          n.c = !!paste0("n_", group_levels[1]),
          n.t = !!paste0("n_", group_levels[2]),
          missing.c = !!paste0("miss_", group_levels[1]),
          missing.t = !!paste0("miss_", group_levels[2])
        ) |>
        dplyr::mutate(
          stddiff = abs(p.t - p.c) /
            sqrt((p.t * (1 - p.t) + p.c * (1 - p.c)) / 2),
          n = n.c + n.t,
          se = sqrt(n / (n.c * n.t) + stddiff^2 / (2 * n)),
          stddiff.l = stddiff - 1.96 * se,
          stddiff.u = stddiff + 1.96 * se
        ) |>
        dplyr::select(
          var,
          p.c,
          p.t,
          missing.c,
          missing.t,
          stddiff,
          stddiff.l,
          stddiff.u
        ) |>
        dplyr::mutate(var = factor(var, levels = vcol_names)) |>
        dplyr::arrange(var) |>
        dplyr::mutate(var = as.character(var))
    },
    error = function(e) {
      stop(
        "Failed to process group levels '",
        group_levels[1],
        "' and '",
        group_levels[2],
        "'. Check that gcol contains exactly 2 levels.",
        call. = FALSE
      )
    }
  )

  # Convert to matrix with rownames
  rst <- as.matrix(result[, -1])
  rownames(rst) <- result$var
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

#' @keywords internal
stddiff_category_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol, type = "category")

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  vcol_names <- all_cols[vcol]

  if (verbose) {
    message("Processing ", length(vcol_names), " categorical variable(s)...")
  }

  # Pivot to long format
  long <- data |>
    dplyr::select(dplyr::all_of(c(gcol, vcol))) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(vcol_names),
      names_to = "var",
      values_to = "x"
    )

  # Count missing values per variable and group
  miss <- long |>
    dplyr::group_by(var, .data[[gcol_name]]) |>
    dplyr::summarise(
      miss = sum(dplyr::if_else(is.na(x), 1L, 0L), na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::collect()

  # Compute proportions per category level (excluding NAs)
  props <- long |>
    dplyr::filter(!is.na(x)) |>
    dplyr::count(var, x, .data[[gcol_name]]) |>
    dplyr::group_by(var, .data[[gcol_name]]) |>
    dplyr::mutate(p = n / sum(n, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    dplyr::collect()

  # Get group levels (sorted)
  group_levels <- sort(unique(props[[gcol_name]]))
  check_binary_group(group_levels, gcol_name)

  # Compute stddiff per variable
  stddiff_list <- lapply(split(props, props$var), function(df) {
    # Pivot to wide format for contingency table
    tab <- df |>
      dplyr::select(x, dplyr::all_of(gcol_name), p) |>
      dplyr::arrange(x, .data[[gcol_name]]) |>
      tidyr::pivot_wider(
        names_from = dplyr::all_of(gcol_name),
        values_from = p,
        values_fill = 0
      ) |>
      dplyr::arrange(x)

    # Extract proportions (exclude first level)
    c_vals <- tab[[group_levels[1]]][-1]
    t_vals <- tab[[group_levels[2]]][-1]
    k <- length(c_vals)

    # Check for sufficient levels
    if (k < 1) {
      warning(
        "Variable ",
        unique(df$var),
        " has < 2 levels after removing reference",
        call. = FALSE
      )
      return(c(stddiff = NA_real_, stddiff.l = NA_real_, stddiff.u = NA_real_))
    }

    # Matrix inversion method from original
    S <- matrix(0, k, k)
    for (i in seq_len(k)) {
      for (j in seq_len(k)) {
        S[i, j] <- if (i == j) {
          0.5 * (t_vals[i] * (1 - t_vals[i]) + c_vals[i] * (1 - c_vals[i]))
        } else {
          -0.5 * (t_vals[i] * t_vals[j] + c_vals[i] * c_vals[j])
        }
      }
    }

    # Try matrix inversion
    tryCatch(
      {
        e <- diag(rep(1, k))
        s <- solve(S, e)
        d <- sqrt(t(t_vals - c_vals) %*% s %*% (t_vals - c_vals))

        # Standard error and CI
        n1 <- sum(df$n[df[[gcol_name]] == group_levels[1]], na.rm = TRUE)
        n2 <- sum(df$n[df[[gcol_name]] == group_levels[2]], na.rm = TRUE)
        n <- n1 + n2
        se <- sqrt(n / (n1 * n2) + d^2 / (2 * n))

        c(
          stddiff = as.numeric(d),
          stddiff.l = as.numeric(d - 1.96 * se),
          stddiff.u = as.numeric(d + 1.96 * se)
        )
      },
      error = function(e) {
        warning(
          "Matrix inversion failed for variable ",
          unique(df$var),
          ". Returning NA.",
          call. = FALSE
        )
        c(stddiff = NA_real_, stddiff.l = NA_real_, stddiff.u = NA_real_)
      }
    )
  })

  stddiff_df <- data.frame(
    var = names(stddiff_list),
    do.call(rbind, stddiff_list),
    row.names = NULL
  )

  # Pivot missing counts wide
  miss_wide <- miss |>
    dplyr::arrange(var, .data[[gcol_name]]) |>
    tidyr::pivot_wider(
      id_cols = var,
      names_from = dplyr::all_of(gcol_name),
      values_from = miss
    ) |>
    dplyr::rename(
      missing.c = !!group_levels[1],
      missing.t = !!group_levels[2]
    )

  # Pivot proportions wide
  props_wide <- props |>
    dplyr::arrange(var, x, .data[[gcol_name]]) |>
    dplyr::select(var, x, dplyr::all_of(gcol_name), p) |>
    tidyr::pivot_wider(
      names_from = dplyr::all_of(gcol_name),
      values_from = p,
      values_fill = 0
    ) |>
    dplyr::rename(
      p.c = !!group_levels[1],
      p.t = !!group_levels[2]
    )

  # Assemble final table
  result <- props_wide |>
    dplyr::left_join(stddiff_df, by = "var") |>
    dplyr::left_join(miss_wide, by = "var") |>
    dplyr::mutate(var = factor(var, levels = vcol_names)) |>
    dplyr::arrange(var, x) |>
    dplyr::mutate(var = as.character(var)) |>
    dplyr::group_by(var) |>
    dplyr::mutate(
      stddiff = dplyr::if_else(dplyr::row_number() == 1, stddiff, NA_real_),
      stddiff.l = dplyr::if_else(dplyr::row_number() == 1, stddiff.l, NA_real_),
      stddiff.u = dplyr::if_else(dplyr::row_number() == 1, stddiff.u, NA_real_),
      missing.c = dplyr::if_else(dplyr::row_number() == 1, missing.c, NA_real_),
      missing.t = dplyr::if_else(dplyr::row_number() == 1, missing.t, NA_real_)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      var,
      x,
      p.c,
      p.t,
      missing.c,
      missing.t,
      stddiff,
      stddiff.l,
      stddiff.u
    )

  # Convert to matrix with combined rownames
  rst <- as.matrix(result[, -(1:2)])
  rownames(rst) <- paste(result$var, result$x)
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

#' @keywords internal
stddiff_numeric_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol, type = "numeric")

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  vcol_names <- all_cols[vcol]

  if (verbose) {
    message("Processing ", length(vcol_names), " numeric variable(s)...")
  }

  # Pivot to long format
  long <- data |>
    dplyr::select(dplyr::all_of(c(gcol, vcol))) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(vcol_names),
      names_to = "var",
      values_to = "x"
    )

  # Aggregate and collect
  stats <- long |>
    dplyr::group_by(var, .data[[gcol_name]]) |>
    dplyr::summarise(
      mean = mean(x, na.rm = TRUE),
      sd = sd(x, na.rm = TRUE),
      n = sum(dplyr::if_else(is.na(x), 0L, 1L), na.rm = TRUE),
      miss = sum(dplyr::if_else(is.na(x), 1L, 0L), na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::collect()

  # Get group levels (sorted)
  group_levels <- sort(unique(stats[[gcol_name]]))
  check_binary_group(group_levels, gcol_name)

  # Pivot to wide format
  stats_wide <- stats |>
    dplyr::arrange(var, .data[[gcol_name]]) |>
    tidyr::pivot_wider(
      id_cols = var,
      names_from = dplyr::all_of(gcol_name),
      values_from = c(mean, sd, n, miss)
    )

  tryCatch(
    {
      result <- stats_wide |>
        dplyr::rename(
          mean.c = !!paste0("mean_", group_levels[1]),
          mean.t = !!paste0("mean_", group_levels[2]),
          sd.c = !!paste0("sd_", group_levels[1]),
          sd.t = !!paste0("sd_", group_levels[2]),
          n.c = !!paste0("n_", group_levels[1]),
          n.t = !!paste0("n_", group_levels[2]),
          missing.c = !!paste0("miss_", group_levels[1]),
          missing.t = !!paste0("miss_", group_levels[2])
        ) |>
        dplyr::mutate(
          stddiff = abs(mean.t - mean.c) / sqrt((sd.t^2 + sd.c^2) / 2),
          n = n.c + n.t,
          se = sqrt(n / (n.c * n.t) + stddiff^2 / (2 * n)),
          stddiff.l = stddiff - 1.96 * se,
          stddiff.u = stddiff + 1.96 * se
        ) |>
        dplyr::select(
          var,
          mean.c,
          sd.c,
          mean.t,
          sd.t,
          missing.c,
          missing.t,
          stddiff,
          stddiff.l,
          stddiff.u
        ) |>
        dplyr::mutate(var = factor(var, levels = vcol_names)) |>
        dplyr::arrange(var) |>
        dplyr::mutate(var = as.character(var))
    },
    error = function(e) {
      stop(
        "Failed to process group levels '",
        group_levels[1],
        "' and '",
        group_levels[2],
        "'. Check that gcol contains exactly 2 levels.",
        call. = FALSE
      )
    }
  )

  # Convert to matrix with rownames
  rst <- as.matrix(result[, -1])
  rownames(rst) <- result$var
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

# Public Wrapper Functions ------------------------------------------------

#' Compute Standardized Differences for Binary Variables (Spark)
#'
#' Calculates standardized differences for binary variables using a Spark
#' DataFrame. This function is equivalent to \code{stddiff::stddiff.binary}
#' but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables
#' @param gcol Unquoted column name for the binary grouping variable
#'   (e.g., treatment vs control)
#' @param vcol Character vector of binary variable column names to analyze
#' @param verbose Logical; if TRUE, prints progress messages. Default is FALSE.
#'
#' @return A numeric matrix with one row per variable and columns:
#' * `p.c`: Proportion in control group (first level alphabetically)
#' * `p.t`: Proportion in treatment group (second level alphabetically)
#' * `missing.c`: Number of missing values in control group
#' * `missing.t`: Number of missing values in treatment group
#' * `stddiff`: Standardized difference
#' * `stddiff.l`: Lower bound of 95% confidence interval
#' * `stddiff.u`: Upper bound of 95% confidence interval
#'
#' @details
#' Variables are encoded using lexicographic ordering since Spark does not
#' have factor types. The first level alphabetically becomes 0, the second
#' becomes 1. For example, "No" < "Yes", "control" < "treatment".
#'
#' The standardized difference is computed as:
#' \deqn{d = \frac{|p_t - p_c|}{\sqrt{(p_t(1-p_t) + p_c(1-p_c))/2}}}
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' # Copy data to Spark
#' spark_df <- copy_to(sc, my_data)
#'
#' # Calculate standardized differences
#' result <- stddiff.binary(
#'   data = spark_df,
#'   gcol = treatment,
#'   vcol = c("var1", "var2", "var3")
#' )
#'
#' spark_disconnect(sc)
#' }
#'
#' @seealso \code{\link{stddiff.category}}, \code{\link{stddiff.numeric}}
#' @export
stddiff.binary <- function(data, gcol, vcol, verbose = FALSE) {
  if (inherits(data, "tbl_spark")) {
    stddiff_binary_spark(data, gcol, vcol, verbose = verbose)
  } else {
    if (!requireNamespace("stddiff", quietly = TRUE)) {
      stop(
        "Package 'stddiff' is required for data.frame. 
        Install with: install.packages('stddiff')",
        call. = FALSE
      )
    }
    if (verbose) {
      message("Using stddiff package for data.frame")
    }
    stddiff::stddiff.binary(data, gcol, vcol)
  }
}

#' Compute Standardized Differences for Categorical Variables (Spark)
#'
#' Calculates standardized differences for categorical variables
#' using a Spark DataFrame. This function is equivalent to
#' \code{stddiff::stddiff.category} but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables
#' @param gcol Unquoted column name for the binary grouping variable
#' @param vcol Character vector of categorical variable column names to analyze
#' @param verbose Logical; if TRUE, prints progress messages. Default is FALSE.
#'
#' @return A numeric matrix with one row per category level and columns:
#' * `p.c`: Proportion in control group
#' * `p.t`: Proportion in treatment group
#' * `missing.c`: Number of missing values in control group (first row only)
#' * `missing.t`: Number of missing values in treatment group (first row only)
#' * `stddiff`: Standardized difference (first row only)
#' * `stddiff.l`: Lower CI bound (first row only)
#' * `stddiff.u`: Upper CI bound (first row only)
#'
#' Row names are formatted as "variable_name level_name".
#'
#' @details
#' For categorical variables with K levels, the standardized difference is
#' computed using a multivariate approach that accounts for all K-1 levels
#' simultaneously (excluding the reference level).
#'
#' Category levels are sorted lexicographically. The first level alphabetically
#' serves as the reference category.
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' spark_df <- copy_to(sc, my_data)
#'
#' result <- stddiff.category(
#'   data = spark_df,
#'   gcol = treatment,
#'   vcol = c("race", "education", "region")
#' )
#'
#' spark_disconnect(sc)
#' }
#'
#' @seealso \code{\link{stddiff.binary}}, \code{\link{stddiff.numeric}}
#' @export
stddiff.category <- function(data, gcol, vcol, verbose = FALSE) {
  if (inherits(data, "tbl_spark")) {
    stddiff_category_spark(data, gcol, vcol, verbose = verbose)
  } else {
    if (!requireNamespace("stddiff", quietly = TRUE)) {
      stop(
        "Package 'stddiff' is required for data.frame. 
        Install with: install.packages('stddiff')",
        call. = FALSE
      )
    }
    if (verbose) {
      message("Using stddiff package for data.frame")
    }
    stddiff::stddiff.category(data, gcol, vcol)
  }
}

#' Compute Standardized Differences for Numeric Variables (Spark)
#'
#' Calculates standardized differences for continuous numeric variables using
#' a Spark DataFrame. This function is equivalent to
#' \code{stddiff::stddiff.numeric} but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables
#' @param gcol Unquoted column name for the binary grouping variable
#' @param vcol Character vector of numeric variable column names to analyze
#' @param verbose Logical; if TRUE, prints progress messages. Default is FALSE.
#'
#' @return A numeric matrix with one row per variable and columns:
#' * `mean.c`: Mean in control group
#' * `sd.c`: Standard deviation in control group
#' * `mean.t`: Mean in treatment group
#' * `sd.t`: Standard deviation in treatment group
#' * `missing.c`: Number of missing values in control group
#' * `missing.t`: Number of missing values in treatment group
#' * `stddiff`: Standardized difference
#' * `stddiff.l`: Lower bound of 95% confidence interval
#' * `stddiff.u`: Upper bound of 95% confidence interval
#'
#' @details
#' The standardized difference for continuous variables is computed as:
#' \deqn{d = \frac{|\bar{x}_t - \bar{x}_c|}{\sqrt{(s_t^2 + s_c^2)/2}}}
#' where \eqn{\bar{x}} represents means and \eqn{s^2} represents variances.
#'
#' This is also known as Cohen's d with pooled standard deviation.
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' spark_df <- copy_to(sc, my_data)
#'
#' result <- stddiff.numeric(
#'   data = spark_df,
#'   gcol = treatment,
#'   vcol = c("age", "weight", "bmi")
#' )
#'
#' spark_disconnect(sc)
#' }
#'
#' @seealso \code{\link{stddiff.binary}}, \code{\link{stddiff.category}}
#' @export
stddiff.numeric <- function(data, gcol, vcol, verbose = FALSE) {
  if (inherits(data, "tbl_spark")) {
    stddiff_numeric_spark(data, gcol, vcol, verbose = verbose)
  } else {
    if (!requireNamespace("stddiff", quietly = TRUE)) {
      stop(
        "Package 'stddiff' is required for data.frame. 
        Install with: install.packages('stddiff')",
        call. = FALSE
      )
    }
    if (verbose) {
      message("Using stddiff package for data.frame")
    }
    stddiff::stddiff.numeric(data, gcol, vcol)
  }
}

utils::globalVariables(c(
  # Column names created/used in dplyr pipelines
  "var", "x", "x_mapped", "min_val", "max_val",
  "p", "n", "miss",
  "p.c", "p.t", "n.c", "n.t",
  "missing.c", "missing.t",
  "mean.c", "mean.t", "sd.c", "sd.t",
  "stddiff", "stddiff.l", "stddiff.u",
  "se",
  ".data", "sd"
))
