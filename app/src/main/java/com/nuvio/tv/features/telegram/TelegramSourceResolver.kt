package com.nuvio.tv.features.telegram

import android.util.Log
import com.nuvio.tv.domain.model.Stream
import com.nuvio.tv.domain.model.StreamBehaviorHints
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeoutOrNull

internal actual object TelegramSourceResolver {
    private const val TAG = "TelegramResolver"
    private const val SCORE_THRESHOLD = 55
    private const val SEARCH_TIMEOUT_MS = 20_000L
    private const val MAX_RESULTS = 50

    actual fun isEnabled(): Boolean = TelegramRepository.isAuthenticated()

    actual suspend fun resolve(
        title: String,
        year: Int?,
        season: Int?,
        episode: Int?,
        imdbId: String,
        isMovie: Boolean
    ): List<Stream> {
        if (!isEnabled()) return emptyList()

        return try {
            withTimeoutOrNull(SEARCH_TIMEOUT_MS) {
                resolveInternal(title, year, season, episode, isMovie)
            } ?: emptyList()
        } catch (e: Exception) {
            Log.w(TAG, "Telegram search error for $title", e)
            emptyList()
        }
    }

    private suspend fun resolveInternal(
        title: String,
        year: Int?,
        season: Int?,
        episode: Int?,
        isMovie: Boolean
    ): List<StreamItem> {
        val queries = if (season != null && episode != null)
            TelegramSearchMatcher.buildSeriesQueries(title, season, episode)
        else
            TelegramSearchMatcher.buildMovieQueries(title, year)

        val seen = mutableSetOf<Pair<String, Long>>()
        val allMessages = mutableListOf<TelegramVideoMessage>()

        coroutineScope {
            queries.map { query ->
                async {
                    try {
                        TelegramRepository.searchVideoMessages(query, MAX_RESULTS)
                    } catch (e: Exception) {
                        Log.e(TAG, "Telegram search failed for query $query", e)
                        emptyList()
                    }
                }
            }.awaitAll().flatten().forEach { msg ->
                if (seen.add(msg.fileName to msg.fileSize)) allMessages.add(msg)
            }
        }

        return allMessages
            .mapNotNull { msg ->
                val score = TelegramSearchMatcher.score(
                    fileName = msg.fileName,
                    caption = msg.caption,
                    title = title,
                    localizedTitle = null,
                    englishTitle = null,
                    year = year,
                    season = season,
                    episode = episode
                )
                if (score < SCORE_THRESHOLD) null else msg
            }
            .map { msg ->
                val streamUrl = TelegramRepository.getStreamUrl(msg.fileId)
                val displayName = if (msg.fileName == "Default_Name.mkv" || msg.fileName == "Default_Name.mp4")
                    msg.caption.takeIf { it.isNotBlank() } ?: msg.fileName
                else msg.fileName
                val quality = parseQuality("${msg.fileName} ${msg.caption}")

                Stream(
                    name = "Telegram",
                    title = displayName,
                    description = displayName,
                    url = streamUrl,
                    ytId = null,
                    infoHash = null,
                    fileIdx = null,
                    externalUrl = null,
                    behaviorHints = StreamBehaviorHints(
                        notWebReady = false,
                        bingeGroup = null,
                        countryWhitelist = null,
                        proxyHeaders = null,
                        videoSize = msg.fileSize,
                        filename = msg.fileName
                    ),
                    addonName = "Telegram",
                    addonLogo = null,
                    quality = quality
                )
            }
            .sortedByDescending { it.behaviorHints.videoSize ?: 0L }
    }

    private fun parseQuality(raw: String): String {
        val t = raw.lowercase().replace(' ', '.')
        fun has(vararg xs: String) = xs.any { it in t }
        return when {
            has("dvdscr", "screener", ".scr.") -> "SCR"
            has(".cam.", "camrip", "hdcam", "hdts", "telesync") -> "CAM"
            has("360", "36o") -> "360p"
            has("480", "48o") -> "480p"
            has("720", "72o") -> "720p"
            has("1080", "1o8o", "108o", "1o80", ".fhd.") -> "1080p"
            has("2160", "216o", ".4k.", ".uhd.", "ultrahd") -> "4K"
            else -> "HD"
        }
    }

    private fun formatBytes(bytes: Long): String = when {
        bytes <= 0 -> ""
        bytes >= 1_000_000_000 -> "%.2f GB".format(bytes / 1_000_000_000.0)
        bytes >= 1_000_000 -> "%.1f MB".format(bytes / 1_000_000.0)
        else -> "%.0f KB".format(bytes / 1_000.0)
    }
}
