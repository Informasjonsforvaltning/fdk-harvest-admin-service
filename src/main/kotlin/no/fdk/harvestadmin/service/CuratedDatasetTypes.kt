package no.fdk.harvestadmin.service

import org.apache.commons.csv.CSVFormat
import java.io.StringReader

private const val FDK_ID_COLUMN = "fdkId"
private const val TITLE_COLUMN = "title"

fun parseCuratedDatasetTypes(csv: String): Map<String, Map<String, String>> {
    val parser =
        CSVFormat.DEFAULT
            .builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true)
            .setIgnoreEmptyLines(true)
            .get()
            .parse(StringReader(csv.removePrefix("﻿")))

    val header = parser.headerNames
    require(header.firstOrNull() == FDK_ID_COLUMN) { "Curated dataset types must start with column $FDK_ID_COLUMN" }
    val codes = header.filter { it != FDK_ID_COLUMN && it != TITLE_COLUMN }

    return parser
        .filter { it.isConsistent && it.get(FDK_ID_COLUMN).isNotBlank() }
        .associate { record -> record.get(FDK_ID_COLUMN) to codes.associateWith { record.get(it).lowercase() } }
}

fun changedFdkIds(previous: String, current: String): Set<String> {
    val before = parseCuratedDatasetTypes(previous)
    val after = parseCuratedDatasetTypes(current)
    return (before.keys + after.keys).filter { before[it] != after[it] }.toSet()
}
