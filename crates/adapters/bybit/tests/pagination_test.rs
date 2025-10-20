// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Tests for bar request pagination logic to ensure chronological ordering.
//!
//! These tests verify that the pagination implementation in `request_bars`:
//! 1. Maintains chronological order when fetching multiple pages (oldest to newest)
//! 2. Correctly applies limit to return the most recent N bars
//! 3. Uses `splice(0..0, new_bars)` to insert older pages at the front

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::{Query, State},
    response::Json,
    routing::get,
};
use chrono::{DateTime, Duration, Utc};
use nautilus_bybit::{
    common::{enums::BybitProductType, parse::parse_linear_instrument},
    http::{
        client::BybitHttpClient, models::BybitFeeRate, query::BybitInstrumentsInfoParamsBuilder,
    },
};
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{InstrumentId, Symbol, Venue},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Mutex, OnceCell},
};

#[derive(Clone)]
struct PaginationTestState {
    request_count: Arc<Mutex<usize>>,
}

impl Default for PaginationTestState {
    fn default() -> Self {
        Self {
            request_count: Arc::new(Mutex::new(0)),
        }
    }
}

// Generate mock kline data with timestamps
fn generate_kline(timestamp_ms: i64, open: &str, high: &str, low: &str, close: &str) -> Value {
    json!([
        timestamp_ms.to_string(),
        open,
        high,
        low,
        close,
        "100.0",    // volume
        "100000.0"  // turnover
    ])
}

// Mock endpoint that simulates pagination
async fn mock_klines_paginated(
    Query(params): Query<HashMap<String, String>>,
    State(_state): State<PaginationTestState>,
) -> Json<Value> {
    let end_ms = params
        .get("end")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| Utc::now().timestamp_millis());

    // Generate bars going backwards from end_ms
    // Each bar is 1 minute apart
    let mut klines = Vec::new();
    for i in 0..1000 {
        let bar_time = end_ms - (i * 60_000);
        klines.push(generate_kline(
            bar_time, "50000.0", "50100.0", "49900.0", "50050.0",
        ));
    }

    Json(json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "linear",
            "symbol": "ETHUSDT",
            "list": klines
        },
        "time": Utc::now().timestamp_millis()
    }))
}

// Mock instrument info endpoint
async fn mock_instruments_info(Query(_params): Query<HashMap<String, String>>) -> Json<Value> {
    Json(json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "nextPageCursor": null,
            "list": [{
                "symbol": "ETHUSDT",
                "contractType": "LinearPerpetual",
                "status": "Trading",
                "baseCoin": "ETH",
                "quoteCoin": "USDT",
                "launchTime": "1699990000000",
                "deliveryTime": "1702592000000",
                "deliveryFeeRate": "0.0005",
                "priceScale": "2",
                "leverageFilter": {
                    "minLeverage": "1",
                    "maxLeverage": "100",
                    "leverageStep": "1"
                },
                "priceFilter": {
                    "minPrice": "0.1",
                    "maxPrice": "100000",
                    "tickSize": "0.05"
                },
                "lotSizeFilter": {
                    "maxOrderQty": "1000.0",
                    "minOrderQty": "0.01",
                    "qtyStep": "0.01",
                    "postOnlyMaxOrderQty": "1000.0",
                    "maxMktOrderQty": "500.0",
                    "minNotionalValue": "5"
                },
                "unifiedMarginTrade": true,
                "fundingInterval": 8,
                "settleCoin": "USDT"
            }]
        },
        "time": Utc::now().timestamp_millis()
    }))
}

async fn start_pagination_test_server() -> Result<(SocketAddr, PaginationTestState), anyhow::Error>
{
    let state = PaginationTestState::default();

    let app = Router::new()
        .route("/v5/market/kline", get(mock_klines_paginated))
        .route("/v5/market/instruments-info", get(mock_instruments_info))
        .with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok((addr, state))
}

static INSTRUMENT_CACHE: OnceCell<()> = OnceCell::const_new();

async fn init_instrument_cache(client: &BybitHttpClient) {
    INSTRUMENT_CACHE
        .get_or_init(|| async {
            // Load instruments and manually add to cache
            let mut params = BybitInstrumentsInfoParamsBuilder::default();
            params.category(BybitProductType::Linear);
            params.symbol("ETHUSDT".to_string());
            let params = params.build().unwrap();

            if let Ok(response) = client.http_get_instruments_linear(&params).await {
                use nautilus_core::time::get_atomic_clock_realtime;
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                for definition in response.result.list {
                    // Create a default fee rate for testing
                    let fee_rate = BybitFeeRate {
                        symbol: definition.symbol,
                        taker_fee_rate: "0.00055".to_string(),
                        maker_fee_rate: "0.0001".to_string(),
                        base_coin: Some(definition.base_coin),
                    };

                    if let Ok(instrument) =
                        parse_linear_instrument(&definition, &fee_rate, ts_init, ts_init)
                    {
                        client.add_instrument(instrument);
                    }
                }
            }
        })
        .await;
}

#[rstest]
#[tokio::test]
async fn test_bars_chronological_order_single_page() {
    let (addr, _state) = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{}", addr);

    let client = BybitHttpClient::new(Some(base_url), Some(60), None, None, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-LINEAR"), Venue::from("BYBIT"));
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Utc::now();
    let start = end - Duration::hours(1);

    let bars = client
        .request_bars(
            BybitProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(100),
            false,
        )
        .await
        .unwrap();

    // Verify we got bars
    assert!(!bars.is_empty());
    assert!(bars.len() <= 100);

    // Verify chronological order (each bar should be later than the previous)
    for i in 1..bars.len() {
        assert!(
            bars[i].ts_event >= bars[i - 1].ts_event,
            "Bars not in chronological order at index {}: {:?} should be >= {:?}",
            i,
            bars[i].ts_event,
            bars[i - 1].ts_event
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_bars_chronological_order_multiple_pages() {
    let (addr, _state) = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{}", addr);

    let client = BybitHttpClient::new(Some(base_url), Some(60), None, None, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-LINEAR"), Venue::from("BYBIT"));
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Utc::now();
    let start = end - Duration::days(2); // Request enough to trigger multiple pages

    let bars = client
        .request_bars(
            BybitProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(1500), // More than one page (1000)
            false,
        )
        .await
        .unwrap();

    // Verify we got approximately the requested number of bars
    assert!(!bars.is_empty());
    // Should get around 1500 bars (might be slightly less due to time boundaries)
    assert!(bars.len() >= 1000, "Expected multiple pages of bars");

    // Verify strict chronological order across all pages
    for i in 1..bars.len() {
        assert!(
            bars[i].ts_event >= bars[i - 1].ts_event,
            "Bars not in chronological order at index {}: bar[{}].ts_event={:?} should be >= bar[{}].ts_event={:?}",
            i,
            i,
            bars[i].ts_event,
            i - 1,
            bars[i - 1].ts_event
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_bars_limit_returns_most_recent() {
    let (addr, _state) = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{}", addr);

    let client = BybitHttpClient::new(Some(base_url), Some(60), None, None, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-LINEAR"), Venue::from("BYBIT"));
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Utc::now();
    let start = end - Duration::days(3); // Request way more than limit

    let bars = client
        .request_bars(
            BybitProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(500), // Limit to 500 bars
            false,
        )
        .await
        .unwrap();

    // Verify we got exactly the limit
    assert_eq!(bars.len(), 500);

    // Verify chronological order
    for i in 1..bars.len() {
        assert!(bars[i].ts_event >= bars[i - 1].ts_event);
    }

    // The last bar should be the most recent (close to end time)
    let last_bar_time = DateTime::from_timestamp_nanos(bars.last().unwrap().ts_event.as_i64());
    let time_diff = (end - last_bar_time).num_minutes().abs();
    assert!(
        time_diff < 100,
        "Last bar should be close to end time, but was {} minutes away",
        time_diff
    );
}
