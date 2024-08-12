# @file augmentDataQualityFiles.R
#
#
# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of AresIndexer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Augment Data Quality Files

#' @name augmentDataQualityFiles
#' @details Updates dq-result.json files with information on changes from the previous release
#'
#' @param sourceFolders A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format (Achilles::exportAO)
#' to be included in the network data quality index.
#'
#'
#'
#'
#' @export
#'
library(jsonlite)
library(dplyr)

loadDataQualityFile <- function(dir) {
  filePath <- file.path(dir, "dq-result.json")
  fromJSON(filePath)
}

augmentDataQualityFiles <- function(sourceFolders) {
  for(sourceFolder in sourceFolders) {
    writeLines(paste0('Augmenting data quality files for: ', basename(sourceFolder)))
    releases <- list.dirs(sourceFolder, recursive = FALSE)
    loadedData <- list()

    for (i in seq_along(releases)) {
      currentReleaseName <- basename(releases[[i]])
      writeLines(paste0('Processing issues delta for release: ',currentReleaseName))
      currentQualityFile <- loadDataQualityFile(releases[i])
      currentChecks <- currentQualityFile$CheckResults
      currentChecks$checkId <- as.character(currentChecks$checkId)

      loadedData[[length(loadedData) + 1]] <- currentChecks

      if (length(loadedData) > 1) {
        previousData <- loadedData[[length(loadedData) - 1]]

        commonCols <- intersect(names(currentChecks), names(previousData))
        currentChecks <- currentChecks[, commonCols, drop = FALSE]
        previousData <- previousData[, commonCols, drop = FALSE]

        mergedData <- currentChecks %>%
          left_join(previousData, by = "checkId", suffix = c("", "_previous"))

        mergedData <- mergedData %>%
          mutate(delta = case_when(
            is.na(failed_previous) & failed == 1 ~ "NEW",
            failed == 1 & failed_previous == 0 ~ "NEW",
            failed == 1 & failed_previous == 1 ~ "EXISTING",
            failed == 0 & failed_previous == 1 ~ "RESOLVED",
            failed == 0 & failed_previous == 0 ~ "STABLE",
            TRUE ~ "STABLE" # Default case if none of the above match
          )) %>%
          select(-ends_with("_previous"))

        currentQualityFile$CheckResults <- mergedData
      } else {
        currentChecks <- currentChecks %>%
          mutate(Status = ifelse(failed == 1, "NEW", "STABLE"))

        currentQualityFile$CheckResults <- currentChecks
      }

      write_json(currentQualityFile, file.path(releases[i], "dq-result.json"))

      # Maintain only the last two loaded datasets
      if (length(loadedData) > 2) {
        loadedData <- loadedData[-1]
      }
    }
  }
}