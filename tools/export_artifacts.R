save_regimen_bundle <- function(workdir) {
  # 1. Load and bundle the main regimen data
  regimens     <- read.delim(file.path(workdir, "regimens.tsv"),             stringsAsFactors = FALSE)
  drugs        <- read.delim(file.path(workdir, "regimens_drugs_deploy.tsv"), stringsAsFactors = FALSE)
  shortStrings <- read.delim(file.path(workdir, "regimens_shortStrings.tsv"), stringsAsFactors = FALSE)
  save(
    regimens,
    drugs,
    shortStrings,
    file = file.path(workdir, "regimens.rda")
  )

  # 2. Process supporting files using the workdir variable
  validdrugs <- read.delim(file.path(workdir, "validdrugs.tsv"), stringsAsFactors = FALSE)
  save(validdrugs, file = file.path(workdir, "validdrugs.rda"))

  regimengroups <- read.delim(file.path(workdir, "regimengroups.tsv"), stringsAsFactors = FALSE)
  save(regimengroups, file = file.path(workdir, "regimengroups.rda"))

  invisible(TRUE)
}

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 1) {
    stop("Usage: Rscript export_artifacts.R <WORKDIR>")
  }
  save_regimen_bundle(args[1])
}