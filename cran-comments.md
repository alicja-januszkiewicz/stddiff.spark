## Update

This is a minor release (1.0 -> 1.1.0). It fixes several cases where results
on Spark data differed from `stddiff`'s, including value ordering, `NaN`
handling and clashes with internal column names, and accepts the column types
`stddiff` accepts, such as dates and date-times. It also raises the sparklyr
requirement to 1.8.4, the oldest release that works with current dbplyr. See
NEWS.md.

## Test environments

* GitHub Actions, Ubuntu 24.04 (rocker/verse:4.5.2), R 4.5.2, OpenJDK 17,
  `R CMD check --as-cran` including the PDF manual:
  * Spark 3.4.4, sparklyr 1.8.4 with dbplyr 2.4.0
  * Spark 4.2.0, current versions of all packages
* win-builder: R-devel
* R-hub v2, `R CMD check --as-cran --no-manual`:
  * windows: R-devel, Windows Server 2022
  * macos-arm64: R-devel, macOS (arm64)
  * macos: R-devel, macOS (x86_64)
  * linux: R-devel, Ubuntu
  * ubuntu-clang: R-devel, Ubuntu
  * ubuntu-gcc12: R-devel, Ubuntu 22.04
  * ubuntu-next: R-patched, Ubuntu
  * ubuntu-release: R-release, Ubuntu
  * nosuggests: R-devel, Fedora, without Suggests (`--no-manual`)

## R CMD check results

0 errors | 0 warnings | 0 notes in every environment above.

## Tests

The tests need a local Apache Spark installation, so they call
`skip_on_cran()` and are skipped on CRAN, win-builder and R-hub. They run on
GitHub Actions against Spark 3.4.4 and 4.2.0.

## Reverse dependencies

There are no reverse dependencies.
