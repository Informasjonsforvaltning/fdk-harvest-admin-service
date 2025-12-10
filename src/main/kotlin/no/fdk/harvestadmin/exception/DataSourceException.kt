package no.fdk.harvestadmin.exception

import org.springframework.http.HttpStatus

open class DataSourceException(
    message: String,
    val status: HttpStatus,
) : RuntimeException(message)

class ValidationException(
    message: String,
) : DataSourceException(message, HttpStatus.BAD_REQUEST)

class ConflictException(
    message: String,
) : DataSourceException(message, HttpStatus.CONFLICT)

class NotFoundException(
    message: String,
) : DataSourceException(message, HttpStatus.NOT_FOUND)

class ForbiddenException(
    message: String,
) : DataSourceException(message, HttpStatus.FORBIDDEN)
