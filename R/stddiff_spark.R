# stddiff_spark.R - Spark implementations of standardized difference functions
#
# These functions replicate the stddiff package interface for Spark DataFrames.
# They perform the same calculations but on distributed data using sparklyr.

# Helper Functions --------------------------------------------------------

#' Validate inputs for stddiff functions
#' @keywords internal
validate_stddiff_inputs <- function(data, gcol, vcol) {
  all_cols <- colnames(data)
  n_cols <- length(all_cols)

  # 1. Check gcol
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

  # 2. Check vcol
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

  # 3. Column Types: those stddiff can process after collect(), whose
  # as.factor() and as.numeric() take strings, numbers, logicals, dates and
  # date-times. collect() also gives strings for calendar intervals and NULL
  # columns. It gives lists or Java references, which stddiff can't process,
  # for the others, such as arrays, binary, TIMESTAMP_NTZ and ANSI intervals.
  accepted_types <- c(
    "StringType",
    "DoubleType",
    "FloatType",
    "DecimalType",
    "IntegerType",
    "LongType",
    "ShortType",
    "ByteType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "CalendarIntervalType",
    "NullType"
  )
  accepted <- "a string, number, logical, date or date-time"
  cols <- unique(c(gcol, vcol))
  types <- spark_types(data, all_cols[cols])
  for (i in seq_along(cols)) {
    if (!types[i] %in% accepted_types) {
      stop(
        if (cols[i] == gcol) "Group column '" else "Column '",
        all_cols[[cols[i]]],
        "' is ",
        types[i],
        " but must be ",
        accepted,
        ".",
        call. = FALSE
      )
    }
  }

  invisible(TRUE)
}

# Levels and codes of x as factor() gives them in stddiff (as.factor() calls
# factor()): the unique values in order(), as strings. So numbers sort
# numerically, strings as sort() orders them, and numbers equal to 15
# significant digits share a level. Missing values get NA, and so does NaN,
# which factor() would keep as a level: sparklyr's copy_to() turns some R NAs
# into NaN, and NaN is never a real category.
factor_codes <- function(x) {
  if (is.double(x)) {
    x[is.nan(x)] <- NA
  }
  y <- unique(x)
  levels <- unique(as.character(y)[order(y)])
  levels <- levels[!is.na(levels)]
  list(levels = levels, codes = match(as.character(x), levels))
}

# Convert values of a column of `spark_type`, cast to strings in Spark, back
# to the type collect() gives them in R, so they sort and print as they would
# in stddiff: "4", not "4.0". Date-times arrive as microseconds (see
# select_internal()).
restore_type <- function(x, spark_type) {
  switch(
    spark_type,
    IntegerType = ,
    LongType = ,
    ShortType = ,
    ByteType = {
      v <- as.numeric(x)
      # LongType values can exceed R's integer range
      if (all(is.na(v) | abs(v) <= .Machine$integer.max)) as.integer(v) else v
    },
    # Spark writes infinite doubles as "Infinity", which as.numeric() rejects
    DoubleType = ,
    FloatType = ,
    DecimalType = as.numeric(sub("Infinity$", "Inf", x)),
    BooleanType = as.logical(x),
    DateType = as.Date(x),
    TimestampType = datetime_from_micros(x),
    x
  )
}

# The control ("c", the first group level) and treatment ("t") arm of each
# group value collected from select_internal(), with the group levels from
# factor_codes(). Results are widened by these labels, so group values never
# become column names. Rows with a missing group get NA. Stops unless the
# group column has exactly two values.
group_arms <- function(group, gcol_name, gcol_type) {
  if (gcol_type == "TimestampType") {
    group <- datetime_from_micros(group)
  }
  g <- factor_codes(group)
  if (length(g$levels) != 2) {
    stop(
      "Group variable '",
      gcol_name,
      "' must have exactly 2 levels. Found ",
      length(g$levels),
      ": ",
      paste(g$levels, collapse = ", "),
      call. = FALSE
    )
  }
  factor(c("c", "t")[g$codes], levels = c("c", "t"))
}

# Spark types of the named columns, without parameters such as DecimalType's
spark_types <- function(data, cols) {
  schema <- sparklyr::sdf_schema(data)
  vapply(
    cols,
    function(col) sub("\\(.*\\)$", "", schema[[col]]$type),
    character(1),
    USE.NAMES = FALSE
  )
}

# Spark expressions for as.numeric() on string column `col` (a symbol), and
# a 0/1 flag for the values it turns into NA with the warning "NAs introduced
# by coercion". Only strings that R and Spark's cast both read as the same
# number are cast: decimals with an optional exponent, and Inf, Infinity and
# NaN in any case, with surrounding whitespace. Anything else is NA without
# reaching the cast, which would error on it in ANSI mode: blank strings
# without the warning, as in R, and the rest with it. That includes unusual
# formats R reads, such as hexadecimal numbers. The patterns are Java regular
# expressions, written without backslashes, which Spark SQL would unescape.
string_as_numeric <- function(col) {
  ws <- "[ \t\n\r\f\v]*"
  number <- paste0(
    "^", ws, "([+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?",
    "|[+-]?(?i:inf|infinity)|(?i:nan))", ws, "$"
  )
  is_number <- bquote(rlike(.(col), .(number)))
  is_blank <- bquote(rlike(.(col), .(paste0("^", ws, "$"))))
  list(
    value = bquote(dplyr::if_else(.(is_number), as.numeric(.(col)), NA_real_)),
    coerced = bquote(
      dplyr::if_else(is.na(.(col)) | .(is_number) | .(is_blank), 0L, 1L)
    )
  )
}

# Select the group and variable columns under internal names: the group as
# `group`, the variables as `v1`, `v2`, ... in order. Only these columns are
# selected and every one is renamed, so no user column name reaches the code
# below and none can clash with the columns it creates. A column can only be
# selected once, so a variable that is the group column itself is added as a
# copy of `group`. The callers map `v1`, ... back to the original names.
select_internal <- function(data, gcol_name, vcol_names) {
  vars <- paste0("v", seq_along(vcol_names))
  is_group <- vcol_names == gcol_name
  cols <- c(gcol_name, vcol_names[!is_group])
  names(cols) <- c("group", vars[!is_group])

  selected <- dplyr::select(data, dplyr::all_of(cols))
  if (any(is_group)) {
    selected <- dplyr::mutate(
      selected,
      dplyr::across(dplyr::all_of("group"), ~.x, .names = vars[is_group])
    )
  }

  # Date-times become microseconds since 1970 (see datetime_from_micros()),
  # and calendar intervals the strings collect() gives for them, "1 days"
  types <- spark_types(data, c(gcol_name, vcol_names))
  internal <- c("group", vars)
  datetimes <- internal[types == "TimestampType"]
  intervals <- internal[types == "CalendarIntervalType"]
  if (length(datetimes) > 0) {
    selected <- dplyr::mutate(
      selected,
      dplyr::across(dplyr::all_of(datetimes), ~ unix_micros(.x))
    )
  }
  if (length(intervals) > 0) {
    selected <- dplyr::mutate(
      selected,
      dplyr::across(dplyr::all_of(intervals), as.character)
    )
  }
  selected
}

# Date-times from the microseconds select_internal() gives them as, built as
# sparklyr's collect() builds them, in R's time zone. Spark's text form could
# differ from R's in format and time zone, and collect() itself counts a
# fraction of a second twice.
datetime_from_micros <- function(x) {
  as.POSIXct(as.numeric(x) / 1e6, origin = "1970-01-01")
}

# Stack the variable columns `vars` of a select_internal() table into long
# format, with columns `group`, `var` (the internal name) and `value`
pivot_long <- function(selected, vars, numeric = FALSE) {
  selected <- if (numeric) {
    # as.numeric() as in stddiff, which then treats NaN as missing: is.na()
    # counts it and na.omit() drops it
    selected |>
      dplyr::mutate(dplyr::across(dplyr::all_of(vars), as.numeric)) |>
      dplyr::mutate(dplyr::across(
        dplyr::all_of(vars),
        ~ dplyr::if_else(isnan(.x), NA_real_, .x)
      ))
  } else {
    dplyr::mutate(selected, dplyr::across(dplyr::all_of(vars), as.character))
  }

  tidyr::pivot_longer(
    selected,
    cols = dplyr::all_of(vars),
    names_to = "var",
    values_to = "value"
  )
}

# Count the rows of each value of each variable in each group, including
# missing values and rows whose group is missing, and code the values as
# stddiff's as.factor() does. One count per distinct value is collected, so a
# high-cardinality column passed by mistake is slow or fails; that's
# accepted, since its result would be meaningless anyway.
# Returns, per variable, a data frame with columns freq, rank (the factor
# code, NA for missing values) and arm, and its factor levels.
code_values <- function(data, gcol_name, var_names, vars) {
  types <- spark_types(data, var_names)
  counts <- select_internal(data, gcol_name, var_names) |>
    pivot_long(vars) |>
    dplyr::count(var, group, value, name = "freq") |>
    dplyr::collect()
  counts$arm <- group_arms(
    counts$group,
    gcol_name,
    spark_types(data, gcol_name)
  )

  # The factor levels come from the whole column, including rows whose
  # group is missing, as in stddiff
  tables <- lapply(seq_along(vars), function(i) {
    df <- counts[counts$var == vars[i], ]
    f <- factor_codes(restore_type(df$value, types[[i]]))
    list(
      tab = data.frame(freq = df$freq, rank = f$codes, arm = df$arm),
      levels = f$levels
    )
  })
  names(tables) <- vars
  tables
}

# Missing values of one variable in each group, from a code_values() table
missing_counts <- function(tab) {
  c(
    sum(tab$freq[is.na(tab$rank) & tab$arm %in% "c"]),
    sum(tab$freq[is.na(tab$rank) & tab$arm %in% "t"])
  )
}

# Mean, SD, count and missing count of one variable in the control and
# treatment arms, from the summaries of each group value. Spark groups by
# exact value, but values equal to 15 significant digits share an arm, as in
# stddiff, so the summaries of an arm's values are combined.
arm_summaries <- function(st) {
  rows <- lapply(c("c", "t"), function(a) {
    miss <- sum(st$miss[st$arm %in% a])
    s <- st[st$arm %in% a & st$n > 0, ]
    n <- sum(s$n)
    if (nrow(s) <= 1) {
      return(c(c(s$mean, NA)[1], c(s$sd, NA)[1], n, miss))
    }
    mean <- sum(s$n * s$mean) / n
    # Spark gives no SD for a single value, which adds no spread
    ss <- sum(
      (s$n - 1) * ifelse(s$n > 1, s$sd^2, 0) + s$n * (s$mean - mean)^2
    )
    c(mean, if (n > 1) sqrt(ss / (n - 1)) else NA, n, miss)
  })
  rows <- do.call(rbind, rows)
  data.frame(mean = rows[, 1], sd = rows[, 2], n = rows[, 3], miss = rows[, 4])
}

# Warn that a variable gets NA where stddiff would stop with an error
warn_na <- function(var_name, e) {
  warning(
    "Could not compute the standardized difference for variable '",
    var_name,
    "' (",
    conditionMessage(e),
    "). Returning NA.",
    call. = FALSE
  )
}


# Main Functions ----------------------------------------------------------

# Each function collects counts or summaries from Spark, then follows
# stddiff's code for each entry of vcol, so its results, quirks and R's own
# warnings (such as "NaNs produced") match stddiff's. Where stddiff would
# stop with an error for a variable, that variable gets NA with a warning.

#' @keywords internal
stddiff_binary_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol)

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  uvcol <- unique(vcol)
  vars <- paste0("v", seq_along(uvcol))
  var_names <- all_cols[uvcol]

  if (verbose) {
    message("Processing ", length(vcol), " binary variable(s)...")
  }

  tables <- code_values(data, gcol_name, var_names, vars)

  rst <- matrix(
    NA_real_,
    length(vcol),
    7,
    dimnames = list(
      all_cols[vcol],
      c("p.c", "p.t", "missing.c", "missing.t", "stddiff", "stddiff.l", "stddiff.u")
    )
  )
  for (i in seq_along(vcol)) {
    tab <- tables[[vars[match(vcol[i], uvcol)]]]$tab
    na <- missing_counts(tab)

    # The complete rows; p is the mean factor code minus 1 in each group
    ok <- !is.na(tab$rank) & !is.na(tab$arm)
    code <- tab$rank[ok]
    freq <- tab$freq[ok]
    arm <- tab$arm[ok]
    p1 <- sum(code[arm == "t"] * freq[arm == "t"]) / sum(freq[arm == "t"]) - 1
    p2 <- sum(code[arm == "c"] * freq[arm == "c"]) / sum(freq[arm == "c"]) - 1

    stddiff <- abs(p1 - p2) / sqrt((p1 * (1 - p1) + p2 * (1 - p2)) / 2)
    n <- c(sum(freq[arm == "c"]), sum(freq[arm == "t"]))
    se <- sqrt(sum(n) / (n[1] * n[2]) + stddiff^2 / (2 * sum(n)))
    rst[i, ] <- c(
      p2,
      p1,
      na,
      stddiff,
      stddiff - 1.96 * se,
      stddiff + 1.96 * se
    )
  }
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

#' @keywords internal
stddiff_category_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol)

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  uvcol <- unique(vcol)
  vars <- paste0("v", seq_along(uvcol))
  var_names <- all_cols[uvcol]

  if (verbose) {
    message("Processing ", length(vcol), " categorical variable(s)...")
  }

  tables <- code_values(data, gcol_name, var_names, vars)

  out_cols <- c(
    "p.c",
    "p.t",
    "missing.c",
    "missing.t",
    "stddiff",
    "stddiff.l",
    "stddiff.u"
  )

  blocks <- lapply(seq_along(vcol), function(i) {
    u <- match(vcol[i], uvcol)
    tab <- tables[[vars[u]]]$tab
    var_levels <- tables[[vars[u]]]$levels
    na <- missing_counts(tab)
    nr <- length(var_levels)

    # A variable with no non-missing values has no levels, so it gets one
    # row named after it, with its missing counts, as in stddiff.numeric()
    if (nr == 0) {
      warn_na(var_names[[u]], simpleError("no non-missing values"))
      block <- matrix(
        NA_real_,
        1,
        length(out_cols),
        dimnames = list(var_names[[u]], out_cols)
      )
      block[1, c("missing.c", "missing.t")] <- na
      return(block)
    }

    # Contingency table of the complete rows: one row per level of the whole
    # column, in factor order, and the control and treatment groups
    ok <- !is.na(tab$rank) & !is.na(tab$arm)
    tbl <- matrix(0, nr, 2)
    for (j in which(ok)) {
      a <- as.integer(tab$arm[j])
      tbl[tab$rank[j], a] <- tbl[tab$rank[j], a] + tab$freq[j]
    }
    prop <- prop.table(tbl, 2)

    # stddiff stops on a single level ("subscript out of bounds")
    stat <- if (nr == 1) {
      warn_na(var_names[[u]], simpleError("only one level"))
      c(NA_real_, NA_real_, NA_real_)
    } else tryCatch(
      {
        # As in stddiff, including its 1:k loops
        t_vals <- prop[-1, 2]
        c_vals <- prop[-1, 1]
        k <- nr - 1
        l <- k
        s <- matrix(rep(0, k * l), ncol = l)
        for (ii in 1:k) {
          for (j in 1:l) {
            if (ii == j) {
              s[ii, j] <- 0.5 *
                (t_vals[ii] * (1 - t_vals[ii]) + c_vals[ii] * (1 - c_vals[ii]))
            }
            if (ii != j) {
              s[ii, j] <- -0.5 * (t_vals[ii] * t_vals[j] + c_vals[ii] * c_vals[j])
            }
          }
        }
        e <- diag(rep(1, k))
        s <- solve(s, e)
        tc <- t_vals - c_vals
        stddiff <- as.numeric(sqrt(t(tc) %*% s %*% tc))
        n <- colSums(tbl)
        se <- sqrt(sum(n) / (n[1] * n[2]) + stddiff^2 / (2 * sum(n)))
        c(stddiff, stddiff - 1.96 * se, stddiff + 1.96 * se)
      },
      error = function(e) {
        warn_na(var_names[[u]], e)
        c(NA_real_, NA_real_, NA_real_)
      }
    )

    # Row names from the factor levels, as in stddiff
    block <- matrix(
      NA_real_,
      nr,
      length(out_cols),
      dimnames = list(paste(var_names[[u]], var_levels), out_cols)
    )
    block[, c("p.c", "p.t")] <- prop
    block[1, c("missing.c", "missing.t")] <- na
    block[1, c("stddiff", "stddiff.l", "stddiff.u")] <- stat
    block
  })

  rst <- do.call(rbind, blocks)
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

#' @keywords internal
stddiff_numeric_spark <- function(data, gcol, vcol, verbose = FALSE) {
  validate_stddiff_inputs(data, gcol, vcol)

  all_cols <- colnames(data)
  gcol_name <- all_cols[gcol]
  uvcol <- unique(vcol)
  vars <- paste0("v", seq_along(uvcol))
  var_names <- all_cols[uvcol]

  if (verbose) {
    message("Processing ", length(vcol), " numeric variable(s)...")
  }

  # stddiff codes the group column as a factor, so as a variable its values
  # are the codes 1 and 2. A constant here has the same SD and counts; the
  # means are set to the codes below.
  selected <- select_internal(data, gcol_name, var_names)
  group_var <- vars[var_names == gcol_name]
  if (length(group_var) > 0) {
    selected <- dplyr::mutate(
      selected,
      dplyr::across(dplyr::all_of(group_var), ~1)
    )
  }

  # Columns are converted as as.numeric() converts them: strings (and the
  # strings select_internal() makes of calendar intervals) as in
  # string_as_numeric(), with a flag column for the values it turns into NA
  # with a warning, and dates and date-times into days and seconds since 1970
  types <- spark_types(data, var_names)
  is_var <- var_names != gcol_name
  string_vars <- vars[
    types %in% c("StringType", "CalendarIntervalType") & is_var
  ]
  # sprintf(), unlike paste0(), gives none for none
  flags <- sprintf("coerced_%s", string_vars)
  convert <- list()
  for (i in seq_along(string_vars)) {
    s <- string_as_numeric(as.name(string_vars[i]))
    # The flag first, from the string, which the value then replaces
    convert[[flags[i]]] <- s$coerced
    convert[[string_vars[i]]] <- s$value
  }
  for (v in vars[types == "DateType" & is_var]) {
    convert[[v]] <- bquote(unix_date(.(as.name(v))))
  }
  # select_internal() gives date-times as microseconds
  for (v in vars[types == "TimestampType" & is_var]) {
    convert[[v]] <- bquote(as.numeric(.(as.name(v))) / 1e6)
  }
  if (length(convert) > 0) {
    selected <- dplyr::mutate(selected, !!!convert)
  }

  # Summaries per variable and group. The flag columns stay alongside every
  # variable in the long format, so each variable's rows count them once.
  flag_sums <- lapply(
    flags,
    function(f) bquote(sum(.(as.name(f)), na.rm = TRUE))
  )
  names(flag_sums) <- flags
  stats <- selected |>
    pivot_long(vars, numeric = TRUE) |>
    dplyr::group_by(var, group) |>
    dplyr::summarise(
      mean = mean(value, na.rm = TRUE),
      sd = sd(value, na.rm = TRUE),
      n = sum(dplyr::if_else(is.na(value), 0L, 1L), na.rm = TRUE),
      miss = sum(dplyr::if_else(is.na(value), 1L, 0L), na.rm = TRUE),
      !!!flag_sums,
      .groups = "drop"
    ) |>
    dplyr::collect()

  coerced <- rep(0, length(vars))
  names(coerced) <- vars
  coerced[string_vars] <- vapply(
    flags,
    function(f) sum(stats[[f]][stats$var == vars[1]]),
    numeric(1)
  )

  stats$arm <- group_arms(
    stats$group,
    gcol_name,
    spark_types(data, gcol_name)
  )

  # stddiff turns the group column itself into numbers when it reaches it in
  # vcol, after which levels() finds no levels and every later variable's
  # missing counts are 0
  group_pos <- match(gcol, vcol)

  rst <- matrix(
    NA_real_,
    length(vcol),
    9,
    dimnames = list(
      all_cols[vcol],
      c(
        "mean.c",
        "sd.c",
        "mean.t",
        "sd.t",
        "missing.c",
        "missing.t",
        "stddiff",
        "stddiff.l",
        "stddiff.u"
      )
    )
  )
  for (i in seq_along(vcol)) {
    u <- match(vcol[i], uvcol)

    # stddiff converts each column with as.numeric() in place, so the
    # coercion warning comes only on the column's first appearance in vcol
    if (coerced[[vars[u]]] > 0 && match(vcol[i], vcol) == i) {
      warning("NAs introduced by coercion", call. = FALSE)
    }

    st <- arm_summaries(stats[stats$var == vars[u], ])
    n <- st$n
    means <- if (vars[u] %in% group_var) c(1, 2) else st$mean
    na <- if (!is.na(group_pos) && i >= group_pos) c(0, 0) else st$miss

    # aggregate() in stddiff drops a group with no complete rows, so the
    # remaining group fills the first position; with none it stops
    present <- which(n > 0)
    if (length(present) == 0) {
      warn_na(var_names[[u]], simpleError("no rows to aggregate"))
      rst[i, c("missing.c", "missing.t")] <- na
      next
    }
    m <- c(means[present], NA)[1:2]
    s <- c(st$sd[present], NA)[1:2]

    stddiff <- abs(m[2] - m[1]) / sqrt((s[2]^2 + s[1]^2) / 2)
    se <- sqrt(sum(n) / (n[1] * n[2]) + stddiff^2 / (2 * sum(n)))
    rst[i, ] <- c(
      m[1],
      s[1],
      m[2],
      s[2],
      na,
      stddiff,
      stddiff - 1.96 * se,
      stddiff + 1.96 * se
    )
  }
  rst <- round(rst, 3)

  if (verbose) {
    message("Complete!")
  }

  return(rst)
}

# Public Wrapper Functions ------------------------------------------------

# Spark data goes to `spark_fn`; anything else to stddiff's function `name`
dispatch <- function(name, spark_fn, data, gcol, vcol, verbose) {
  if (inherits(data, "tbl_spark")) {
    return(spark_fn(data, gcol, vcol, verbose = verbose))
  }
  if (!requireNamespace("stddiff", quietly = TRUE)) {
    stop(
      "Package 'stddiff' is required for data.frame. ",
      "Install with: install.packages('stddiff')",
      call. = FALSE
    )
  }
  if (verbose) {
    message("Using stddiff package for data.frame")
  }
  getExportedValue("stddiff", name)(data, gcol, vcol)
}

#' Compute Standardized Differences for Binary Variables (Spark)
#'
#' Calculates standardized differences for binary variables using a Spark
#' DataFrame. Equivalent to \code{stddiff::stddiff.binary} but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables.
#' @param gcol Integer; column index of the binary grouping variable
#'   (e.g., treatment vs control).
#' @param vcol Integer vector; column indices of the binary variables to analyze.
#'   Repeated indices give repeated rows, and \code{gcol} may be included like
#'   any other column, as in \code{stddiff}.
#' @param verbose Logical; if TRUE, prints progress messages. Default is FALSE.
#'
#' @return A numeric matrix with one row per variable and columns:
#' * `p.c`: Proportion in control group (first value of the group column)
#' * `p.t`: Proportion in treatment group (second value of the group column)
#' * `missing.c`: Number of missing values in control group
#' * `missing.t`: Number of missing values in treatment group
#' * `stddiff`: Standardized difference
#' * `stddiff.l`: Lower bound of 95% confidence interval
#' * `stddiff.u`: Upper bound of 95% confidence interval
#'
#' @details
#' Spark DataFrames have no factor type, so values are ordered the way
#' \code{as.factor()} orders them in \code{stddiff}: numerically for numeric
#' columns, alphabetically for strings, and \code{FALSE} before \code{TRUE}.
#' The first value of each variable becomes 0 and the second becomes 1, so
#' \code{p.c} and \code{p.t} are proportions of the second value. The group
#' column is ordered the same way, and its first value is the control group.
#'
#' Like \code{stddiff}, this does not check that variables are binary. A
#' variable with more than two distinct values is coded 0, 1, 2, ... in that
#' order, so its results are not meaningful: the proportions can exceed 1, and
#' the standardized difference can be \code{NaN}, with R's "NaNs produced"
#' warning. Each distinct value is collected, so don't pass continuous or
#' ID-like columns.
#'
#' Differences from \code{stddiff} on Spark data: the group column must have
#' exactly two values, or the function stops with an error. \code{stddiff}
#' doesn't check this, and with more than two groups it silently compares the
#' first two. \code{NaN} counts as missing, where \code{stddiff} keeps it as a
#' value, because sparklyr's \code{copy_to()} turns some R \code{NA}s into
#' \code{NaN}. With more than about 46,000 rows per group, the confidence
#' limits are computed where \code{stddiff}'s integer counts overflow and give
#' \code{NA}. Spark also has no factor type, so the data cannot carry a
#' custom level order. Groups and values follow their sorted order, which is
#' the default order in \code{stddiff}. Local data frames are passed to
#' \code{stddiff}, which uses the level order of factor columns.
#'
#' The standardized difference is computed as:
#' \deqn{d = \frac{|p_t - p_c|}{\sqrt{(p_t(1-p_t) + p_c(1-p_c))/2}}}
#'
#' @examplesIf requireNamespace("sparklyr", quietly = TRUE) && interactive()
#' sc <- sparklyr::spark_connect(master = "local")
#'
#' spark_df <- sparklyr::copy_to(sc, mtcars)
#'
#' result <- stddiff.binary(
#'   data = spark_df,
#'   gcol = 9,   # column index of grouping variable
#'   vcol = c(8) # columns of binary variables
#' )
#'
#' sparklyr::spark_disconnect(sc)
#'
#' @seealso \code{\link{stddiff.category}}, \code{\link{stddiff.numeric}}
#' @export
stddiff.binary <- function(data, gcol, vcol, verbose = FALSE) {
  dispatch("stddiff.binary", stddiff_binary_spark, data, gcol, vcol, verbose)
}

#' Compute Standardized Differences for Categorical Variables (Spark)
#'
#' Calculates standardized differences for categorical variables using a Spark
#' DataFrame. Equivalent to \code{stddiff::stddiff.category} but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables.
#' @param gcol Integer; column index of the binary grouping variable.
#' @param vcol Integer vector; column indices of the categorical variables to analyze.
#'   Repeated indices give repeated rows, and \code{gcol} may be included like
#'   any other column, as in \code{stddiff}.
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
#' Row names are formatted as "variable_name level_name". A variable with no
#' non-missing values gets one row, named after the variable.
#'
#' @details
#' For categorical variables with K levels, the standardized difference is
#' computed using a multivariate approach that accounts for all K-1 levels
#' simultaneously (excluding the reference level). Levels are ordered the way
#' \code{as.factor()} orders them in \code{stddiff} (numerically for numeric
#' columns, alphabetically for strings), and the first level serves as the
#' reference. The standardized difference does not depend on which level is
#' the reference. Each distinct value is collected, so don't pass continuous
#' or ID-like columns.
#'
#' Differences from \code{stddiff} on Spark data: where \code{stddiff} stops
#' with an error for a variable, such as a singular matrix, that variable gets
#' \code{NA} with a warning and the other variables still get results. Local
#' data frames are passed to \code{stddiff}, so the error remains there. The
#' group column must have exactly two values, or the function stops with an
#' error that says so. \code{stddiff} doesn't check this, and with more than
#' two groups it stops with an unrelated error. \code{NaN} counts as missing,
#' where \code{stddiff} keeps it as a level, because sparklyr's
#' \code{copy_to()} turns some R \code{NA}s into \code{NaN}. With more than
#' about 46,000 rows per group, the confidence limits are computed where
#' \code{stddiff}'s integer counts overflow and give \code{NA}. Spark also has
#' no factor type, so the data cannot carry a custom level order: groups and
#' levels follow their sorted order, which is the default order in
#' \code{stddiff}.
#'
#' @examplesIf requireNamespace("sparklyr", quietly = TRUE) && interactive()
#' sc <- sparklyr::spark_connect(master = "local")
#'
#' spark_df <- sparklyr::copy_to(sc, as.data.frame(Titanic))
#'
#' result <- stddiff.category(
#'   data = spark_df,
#'   gcol = 4,   # column index of grouping variable
#'   vcol = c(1) # columns of categorical variables
#' )
#'
#' sparklyr::spark_disconnect(sc)
#'
#' @seealso \code{\link{stddiff.binary}}, \code{\link{stddiff.numeric}}
#' @export
stddiff.category <- function(data, gcol, vcol, verbose = FALSE) {
  dispatch(
    "stddiff.category", stddiff_category_spark, data, gcol, vcol, verbose
  )
}

#' Compute Standardized Differences for Numeric Variables (Spark)
#'
#' Calculates standardized differences for continuous numeric variables using a
#' Spark DataFrame. Equivalent to \code{stddiff::stddiff.numeric} but operates on Spark data.
#'
#' @param data A Spark DataFrame (\code{tbl_spark}) containing the variables.
#' @param gcol Integer; column index of the binary grouping variable.
#' @param vcol Integer vector; column indices of the numeric variables to analyze.
#'   Repeated indices give repeated rows, and \code{gcol} may be included like
#'   any other column, as in \code{stddiff}.
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
#' Differences from \code{stddiff} on Spark data: where \code{stddiff} stops
#' with an error for a variable, as when it has no non-missing values, that
#' variable gets \code{NA} with a warning and the other variables still get
#' results. Local data frames are passed to \code{stddiff}, so the error
#' remains there. The group column must have exactly two values, or the
#' function stops with an error. \code{stddiff} doesn't check this, and with
#' more than two groups it silently compares the first two. Strings are
#' converted as \code{as.numeric()} converts decimals, \code{Inf} and
#' \code{NaN}, but unusual formats it also reads, such as hexadecimal
#' (\code{"0x1A"}), become \code{NA} with its coercion warning. With more than
#' about 46,000 rows per group, the confidence limits are computed where
#' \code{stddiff}'s integer counts overflow and give \code{NA}. Spark also has
#' no factor type, so the data cannot carry a custom level order: groups
#' follow their sorted order, which is the default order in \code{stddiff}.
#'
#' @examplesIf requireNamespace("sparklyr", quietly = TRUE) && interactive()
#' sc <- sparklyr::spark_connect(master = "local")
#'
#' spark_df <- sparklyr::copy_to(sc, mtcars)
#'
#' result <- stddiff.numeric(
#'   data = spark_df,
#'   gcol = 8,          # column index of grouping variable
#'   vcol = c(1, 2, 5)  # columns of numeric variables
#' )
#'
#' sparklyr::spark_disconnect(sc)
#'
#' @seealso \code{\link{stddiff.binary}}, \code{\link{stddiff.category}}
#' @export
stddiff.numeric <- function(data, gcol, vcol, verbose = FALSE) {
  dispatch("stddiff.numeric", stddiff_numeric_spark, data, gcol, vcol, verbose)
}

# Column names created/used in dplyr pipelines
utils::globalVariables(c(
  "group",
  "var",
  "value",
  "sd"
))

# Spark SQL functions, and bquote()'s .() in the expressions that use them
utils::globalVariables(c(
  "isnan",
  "rlike",
  "unix_date",
  "unix_micros",
  "."
))
