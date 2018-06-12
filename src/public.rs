use chrono::{DateTime, Utc, SecondsFormat};

use hyper::header::HeaderValue;
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::header;
use hyper::rt::Future;
use hyper::{Uri, Request, Response, Body, Chunk};
use futures::{Async, Poll, Stream};

use curl::easy::Easy;
use serde::Deserialize;
use serde_json::{de, Number};
use uuid::Uuid;

use super::Error;
use super::ApiError;
use super::Side;

const PUBLIC_API_URL: &'static str = "https://api.gdax.com";

pub enum Level {
    Best    = 1,
    Top50   = 2,
    Full    = 3
}

/** TODO
Should reinstate the automatic conversion to fp64 that was in original to maintain compatibility.
Can add a '_raw' api maybe? Or something else to more closely mirror types gdax uses.
*/

#[derive(Deserialize, Debug)]
pub struct Product {
    pub id: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub base_min_size: String,
    pub base_max_size: String,
    pub quote_increment: String,
    pub status: String,
    pub margin_enabled: bool,
    pub min_market_funds: String,
    pub max_market_funds: String,
    pub post_only: bool,
    pub limit_only: bool,
    pub cancel_only: bool,
}

#[derive(Deserialize, Debug)]
pub struct BookEntry {
    pub price: String,
    pub size: String,
    pub num_orders: u64
}

#[derive(Deserialize, Debug)]
pub struct FullBookEntry {
    pub price: String,
    pub size: String,
    pub order_id: Uuid
}

#[derive(Deserialize, Debug)]
pub struct OrderBook<T> {
    pub sequence: usize,
    pub bids: Vec<T>,
    pub asks: Vec<T>
}

#[derive(Deserialize, Debug)]
pub struct Tick {
    pub trade_id: u64,
    pub price: String,
    pub size: String,
    pub bid: String,
    pub ask: String,
    pub volume: String,
    pub time: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    pub time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: String,
    pub size: String,
    pub side: Side,
}

#[derive(Deserialize, Debug)]
pub struct Candle {
    pub time: u64,
    pub low: f64,
    pub high: f64,
    pub open: f64,
    pub close: f64,
    pub volume: f64
}

#[derive(Deserialize, Debug)]
pub struct Stats {
    pub open: String,
    pub high: String,
    pub low: String,
    pub volume: String,
    pub last: String,
    pub volume_30day: String,
}

#[derive(Deserialize, Debug)]
pub struct Currency {
    pub id: String,
    pub name: String,
    pub min_size: String
}

#[derive(Deserialize, Debug)]
pub struct Time {
    pub iso: DateTime<Utc>,
    pub epoch: f64
}

pub struct Client {
    curl: Easy,
    http_client: HttpClient<HttpConnector>,
}

impl Client {
    pub fn new() -> Client {
        Client {
            curl: Easy::new(),

            http_client: HttpClient::new()
        }
    }

    fn get_and_decode<T>(&mut self, url: &str) -> Result<T, Error>
        where for<'de> T: Deserialize<'de>
    {
        self.curl.url(url).unwrap();
        self.curl.useragent("rust-gdax-client/1.2.0").unwrap();

        let mut buf = Vec::new();

        {
            let mut t = self.curl.transfer();
            t.write_function(|data| {
                buf.extend_from_slice(data);
                Ok(data.len())
            }).unwrap();
            t.perform().unwrap();
        }

        if self.curl.response_code().unwrap() != 200 {
            return Err(Error::Api(ApiError{ message: String::from_utf8(buf).unwrap()}));
        } else {
            return Ok(de::from_reader(&mut buf.as_slice())?)
        }
    }

    pub fn get_products(&mut self) -> Result<Vec<Product>, Error> {
        self.get_and_decode(&format!("{}/products", PUBLIC_API_URL))
    }

    pub fn get_best_order(&mut self, product: &str) -> Result<OrderBook<BookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Best as u8))
    }

    pub fn get_top50_orders(&mut self, product: &str) -> Result<OrderBook<BookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Top50 as u8))
    }

    pub fn get_full_book(&mut self, product: &str) -> Result<OrderBook<FullBookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Full as u8))
    }

    pub fn get_product_ticker(&mut self, product: &str) -> Result<Tick, Error> {
        self.get_and_decode(&format!("{}/products/{}/ticker", PUBLIC_API_URL, product))
    }

    pub fn get_trades(&mut self, product: &str) -> Result<Vec<Trade>, Error> {
        self.get_and_decode(&format!("{}/products/{}/trades", PUBLIC_API_URL, product))
    }

    pub fn get_historic_rates(&mut self,
                              product: &str,
                              start_time: DateTime<Utc>,
                              end_time: DateTime<Utc>,
                              granularity: u64)
        -> Result<Vec<Candle>, Error> {

        self.get_and_decode(&format!("{}/products/{}/candles?start={}&end={}&granularity={}",
                                     PUBLIC_API_URL,
                                     product,
                                     start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                                     end_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                                     granularity))
    }

    pub fn get_24hr_stats(&mut self, product: &str) -> Result<Stats, Error> {
        self.get_and_decode(&format!("{}/products/{}/stats", PUBLIC_API_URL, product))
    }

    pub fn get_currencies(&mut self) -> Result<Vec<Currency>, Error> {
        self.get_and_decode(&format!("{}/currencies", PUBLIC_API_URL))
    }

    pub fn get_time(&mut self) -> Result<Time, Error> {
        self.get_and_decode(&format!("{}/time", PUBLIC_API_URL))
    }
}
