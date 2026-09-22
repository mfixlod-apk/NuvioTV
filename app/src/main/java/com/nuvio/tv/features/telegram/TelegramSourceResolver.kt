package com.nuvio.tv.features.telegram

import com.nuvio.tv.domain.model.Stream

internal expect object TelegramSourceResolver {
    fun isEnabled(): Boolean
    suspend fun resolve(
        title: String,
        year: Int?,
        season: Int? = null,
        episode: Int? = null,
        imdbId: String = "",
        isMovie: Boolean = true
    ): List<Stream>
}
