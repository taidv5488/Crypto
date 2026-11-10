package com.example.promulti

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.promulti.ui.theme.ProMultiTheme
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.GET
import retrofit2.http.Query
import java.text.NumberFormat
import java.util.*
import kotlin.time.Duration.Companion.seconds

data class BinanceTicker(
    @SerializedName("lastPrice") val lastPrice: String?
)

interface BinanceApi {
    @GET("api/v3/ticker/24hr")
    suspend fun getTicker(@Query("symbol") symbol: String): BinanceTicker
}

interface CoinGeckoApi {
    @GET("api/v3/simple/price")
    suspend fun getPrice(
        @Query("ids") ids: String,
        @Query("vs_currencies") currencies: String = "usd"
    ): Map<String, Map<String, Double>>
}

interface CryptoCompareApi {
    @GET("data/price")
    suspend fun getPrice(
        @Query("fsym") fsym: String,
        @Query("tsyms") tsyms: String = "USD"
    ): Map<String, Any>
}

object RetrofitInstance {
    val binance: BinanceApi = Retrofit.Builder()
        .baseUrl("https://api.binance.com/")
        .addConverterFactory(GsonConverterFactory.create())
        .build()
        .create(BinanceApi::class.java)

    val coinGecko: CoinGeckoApi = Retrofit.Builder()
        .baseUrl("https://api.coingecko.com/")
        .addConverterFactory(GsonConverterFactory.create())
        .build()
        .create(CoinGeckoApi::class.java)

    val cryptoCompare: CryptoCompareApi = Retrofit.Builder()
        .baseUrl("https://min-api.cryptocompare.com/")
        .addConverterFactory(GsonConverterFactory.create())
        .build()
        .create(CryptoCompareApi::class.java)
}

data class PlatformQuote(
    val platformName: String,
    val price: Double
)

data class CryptoItem(
    val symbol: String,
    val coinGeckoId: String,
    val name: String,
    val bestPrice: Double = 0.0,
    val bestPlatform: String = "",
    val quotes: List<PlatformQuote> = emptyList(),
    val statusMessage: String = "Đang tải..."
)

class CryptoViewModel : ViewModel() {

    private val _cryptoList = MutableStateFlow(
        listOf(
            CryptoItem(symbol = "BTC", coinGeckoId = "bitcoin", name = "Bitcoin"),
            CryptoItem(symbol = "ETH", coinGeckoId = "ethereum", name = "Ethereum"),
            CryptoItem(symbol = "LTC", coinGeckoId = "litecoin", name = "Litecoin"),
            CryptoItem(symbol = "XMR", coinGeckoId = "monero", name = "Monero")
        )
    )
    val cryptoList: StateFlow<List<CryptoItem>> = _cryptoList.asStateFlow()

    init {
        startRealtimePriceCollector()
    }
    private suspend fun fetchFromBinance(symbol: String): PlatformQuote? {
        return try {
            val response = RetrofitInstance.binance.getTicker("${symbol}USDT")
            val price = response.lastPrice?.toDoubleOrNull() ?: 0.0
            if (price > 0.0) PlatformQuote("Binance", price) else null
        } catch (_: Exception) {
            null
        }
    }
    private suspend fun fetchFromCryptoCompare(symbol: String): PlatformQuote? {
        return try {
            val response = RetrofitInstance.cryptoCompare.getPrice(fsym = symbol)
            val price = when (val usdPrice = response["USD"]) {
                is Number -> usdPrice.toDouble()
                is String -> usdPrice.toDoubleOrNull() ?: 0.0
                else -> 0.0
            }
            if (price > 0.0) PlatformQuote("CryptoCompare", price) else null
        } catch (_: Exception) {
            null
        }
    }
    private suspend fun fetchFromCoinGecko(coinGeckoIds: List<String>): Map<String, Double> {
        return try {
            val idsParam = coinGeckoIds.joinToString(",")
            val response = RetrofitInstance.coinGecko.getPrice(ids = idsParam)
            val resultMap = mutableMapOf<String, Double>()
            response.forEach { (id, prices) ->
                prices["usd"]?.let { price -> resultMap[id] = price }
            }
            resultMap
        } catch (_: Exception) {
            emptyMap()
        }
    }

    // Bất động bộ
    private suspend fun aggregatePrices(
        coin: CryptoItem,
        coinGeckoBatchMap: Map<String, Double>
    ): CryptoItem = coroutineScope {
        val binanceDeferred = async(Dispatchers.IO) { fetchFromBinance(coin.symbol) }
        val cryptoCompareDeferred = async(Dispatchers.IO) { fetchFromCryptoCompare(coin.symbol) }

        val binanceQuote = binanceDeferred.await()
        val cryptoCompareQuote = cryptoCompareDeferred.await()

        val coinGeckoPrice = coinGeckoBatchMap[coin.coinGeckoId]
        val coinGeckoQuote = if (coinGeckoPrice != null && coinGeckoPrice > 0.0) {
            PlatformQuote("CoinGecko", coinGeckoPrice)
        } else null

        val lastQuotesMap = coin.quotes.associateBy { it.platformName }
        val finalBinance = binanceQuote ?: lastQuotesMap["Binance"]
        val finalCoinGecko = coinGeckoQuote ?: lastQuotesMap["CoinGecko"]
        val finalCryptoCompare = cryptoCompareQuote ?: lastQuotesMap["CryptoCompare"]

        val validQuotes = listOfNotNull(finalBinance, finalCoinGecko, finalCryptoCompare)

        if (validQuotes.isNotEmpty()) {
            val best = validQuotes.minByOrNull { it.price } ?: validQuotes.first()
            coin.copy(
                bestPrice = best.price,
                bestPlatform = best.platformName,
                quotes = validQuotes,
                statusMessage = "Cập nhật thành công"
            )
        } else {
            coin.copy(statusMessage = "Đang chờ kết nối sàn...")
        }
    }
    private fun startRealtimePriceCollector() {
        viewModelScope.launch(Dispatchers.Default) {
            while (isActive) {
                val currentList = _cryptoList.value
                val allCoinGeckoIds = currentList.map { it.coinGeckoId }

                val coinGeckoBatchMap = withContext(Dispatchers.IO) {
                    fetchFromCoinGecko(allCoinGeckoIds)
                }

                // Đồng bộ kết hợp cho từng coin
                val updatedList = currentList.map { coin ->
                    async { aggregatePrices(coin, coinGeckoBatchMap) }
                }.awaitAll()

                _cryptoList.value = updatedList
                delay(15.seconds) // vì giới hạn api và rate limiter
            }
        }
    }
}

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            ProMultiTheme {
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = Color(0xFF131722)
                ) {
                    CryptoScreen()
                }
            }
        }
    }
}

@Composable
fun CryptoScreen(viewModel: CryptoViewModel = viewModel()) {
    val cryptoList by viewModel.cryptoList.collectAsState()
    var clickCount by remember { mutableIntStateOf(0) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .statusBarsPadding()
            .navigationBarsPadding()
            .padding(16.dp)
    ) {
        Text(
            text = "Crypto Price Aggregator",
            fontSize = 20.sp,
            fontWeight = FontWeight.Bold,
            color = Color.White
        )
        Text(
            text = "Binance • CoinGecko • CryptoCompare",
            fontSize = 13.sp,
            color = Color.Gray,
            modifier = Modifier.padding(bottom = 12.dp)
        )

        Card(
            shape = RoundedCornerShape(12.dp),
            colors = CardDefaults.cardColors(containerColor = Color(0xFF1E222D)),
            modifier = Modifier
                .fillMaxWidth()
                .padding(bottom = 16.dp)
        ) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(12.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Column(modifier = Modifier.weight(1f)) {
                    Text("Main Thread:",
                        color = Color.White,
                        fontSize = 12.sp)
                    Text("Số lần click: $clickCount",
                        color = Color(0xFF26A69A),
                        fontWeight = FontWeight.Bold)
                }
                Button(
                    onClick = { clickCount++ },
                    colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF2962FF))
                ) {
                    Text("Bấm thử")
                }
            }
        }

        LazyColumn(
            verticalArrangement = Arrangement.spacedBy(12.dp),
            modifier = Modifier.fillMaxSize()
        ) {
            items(cryptoList, key = { it.symbol }) { coin ->
                RealCryptoCard(coin = coin)
            }
        }
    }
}

@Composable
fun RealCryptoCard(coin: CryptoItem) {
    val formatter = remember { NumberFormat.getCurrencyInstance(Locale.US) }

    Card(
        shape = RoundedCornerShape(16.dp),
        colors = CardDefaults.cardColors(containerColor = Color(0xFF1E222D)),
        modifier = Modifier.fillMaxWidth()
    ) {
        Column(modifier = Modifier.padding(16.dp)) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Column {
                    Text(coin.symbol,
                        fontWeight = FontWeight.Bold,
                        color = Color.White,
                        fontSize = 18.sp)
                    Text(coin.name,
                        color = Color.Gray,
                        fontSize = 12.sp)
                }

                Column(horizontalAlignment = Alignment.End) {
                    if (coin.bestPrice > 0.0) {
                        Text(
                            text = formatter.format(coin.bestPrice),
                            color = Color(0xFF26A69A),
                            fontWeight = FontWeight.ExtraBold,
                            fontSize = 18.sp
                        )
                        Text(
                            text = "Tốt nhất tại: ${coin.bestPlatform}",
                            color = Color.LightGray,
                            fontSize = 11.sp
                        )
                    } else {
                        Text(coin.statusMessage,
                            color = Color.Yellow,
                            fontSize = 13.sp)
                    }
                }
            }

            Spacer(modifier = Modifier.height(10.dp))
            HorizontalDivider(color = Color(0xFF2A2E39))
            Spacer(modifier = Modifier.height(8.dp))

            Text("Giá từ 3 sàn:",
                fontSize = 11.sp,
                color = Color.Gray)
            Spacer(modifier = Modifier.height(6.dp))

            if (coin.quotes.isNotEmpty()) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    coin.quotes.forEach { quote ->
                        val isBest = quote.platformName == coin.bestPlatform
                        Column {
                            Text(
                                text = quote.platformName,
                                fontSize = 11.sp,
                                color = if (isBest) Color(0xFF26A69A) else Color.White
                            )
                            Text(
                                text = formatter.format(quote.price),
                                fontSize = 12.sp,
                                fontWeight = if (isBest) FontWeight.Bold else FontWeight.Normal,
                                color = if (isBest) Color(0xFF26A69A) else Color.LightGray
                            )
                        }
                    }
                }
            } else {
                Text("Đang kết nối API mạng...",
                    fontSize = 11.sp,
                    color = Color.Gray)
            }
        }
    }
}

@Preview(showBackground = true)
@Composable
fun GreetingPreview() {
    ProMultiTheme {
        CryptoScreen()
    }
}