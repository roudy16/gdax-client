use chrono::{DateTime, Utc};
use hyper::header::HeaderValue;
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::header;
use hyper::rt::Future;
use hyper::{Uri, Request, Response, Body, Chunk};
use futures::{Async, Poll, Stream};
use serde::Deserialize;
use serde_json::de;
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

#[derive(Deserialize, Debug)]
pub struct Product {
    pub id: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub base_min_size: f64,
    pub base_max_size: f64,
    pub quote_increment: f64
}

#[derive(Deserialize, Debug)]
pub struct BookEntry {
    pub price: f64,
    pub size: f64,
    pub num_orders: u64
}

#[derive(Deserialize, Debug)]
pub struct FullBookEntry {
    pub price: f64,
    pub size: f64,
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
    pub price: f64,
    pub size: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: f64,
    pub time: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    pub time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: f64,
    pub size: f64,
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
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64
}

#[derive(Deserialize, Debug)]
pub struct Currency {
    pub id: String,
    pub name: String,
    pub min_size: f64
}

#[derive(Deserialize, Debug)]
pub struct Time {
    pub iso: DateTime<Utc>,
    pub epoch: f64
}

pub struct Client {
    http_client: HttpClient<HttpConnector>,
}

impl Client {
    pub fn new() -> Client {
        Client {
            http_client: HttpClient::new()
        }
    }

    fn get_and_decode<T>(&self, url: &str) -> Result<T, Error>
        where for<'de> T: Deserialize<'de>
    {
        let uri = url.parse::<Uri>().unwrap();
        let mut req_builder = Request::get(uri);
        req_builder.header(header::USER_AGENT, HeaderValue::from_static("rust-gdax-client/0.1.0"));

        let req = self.http_client.request(req_builder.body(Body::from(""))?);

        let mut res: Response<Body> = match req.wait() {
            Ok(val) => val,
            Err(e) => return Err(Error::from(e)),
        };

        let content: String = match res.body_mut().wait().next() {
            Some(val) => String::from_utf8(val.unwrap().to_vec()).unwrap(),
            None => String::from(""),
        };

        if !res.status().is_success() {
            return Err(Error::Api(ApiError{ message: content}));
        } else {
            return Ok(de::from_reader(&mut content.as_bytes())?)
        }
    }

    pub fn get_products(&self) -> Result<Vec<Product>, Error> {
        self.get_and_decode(&format!("{}/products", PUBLIC_API_URL))
    }

    pub fn get_best_order(&self, product: &str) -> Result<OrderBook<BookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Best as u8))
    }

    pub fn get_top50_orders(&self, product: &str) -> Result<OrderBook<BookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Top50 as u8))
    }

    pub fn get_full_book(&self, product: &str) -> Result<OrderBook<FullBookEntry>, Error> {
        self.get_and_decode(&format!("{}/products/{}/book?level={}",
                                     PUBLIC_API_URL,
                                     product,
                                     Level::Full as u8))
    }

    pub fn get_product_ticker(&self, product: &str) -> Result<Tick, Error> {
        self.get_and_decode(&format!("{}/products/{}/ticker", PUBLIC_API_URL, product))
    }

    pub fn get_trades(&self, product: &str) -> Result<Vec<Trade>, Error> {
        self.get_and_decode(&format!("{}/products/{}/trades", PUBLIC_API_URL, product))
    }

    // XXX: Returns invalid interval?
    pub fn get_historic_rates(&self,
                              product: &str,
                              start_time: DateTime<Utc>,
                              end_time: DateTime<Utc>,
                              granularity: u64)
        -> Result<Vec<Candle>, Error> {

        self.get_and_decode(&format!("{}/products/{}/candles?start={}&end={}&granularity={}",
                                     PUBLIC_API_URL,
                                     product,
                                     start_time.to_rfc3339(),
                                     end_time.to_rfc3339(),
                                     granularity))
    }

    pub fn get_24hr_stats(&self, product: &str) -> Result<Stats, Error> {
        self.get_and_decode(&format!("{}/products/{}/stats", PUBLIC_API_URL, product))
    }

    pub fn get_currencies(&self) -> Result<Vec<Currency>, Error> {
        self.get_and_decode(&format!("{}/currencies", PUBLIC_API_URL))
    }

    pub fn get_time(&self) -> Result<Time, Error> {
        self.get_and_decode(&format!("{}/time", PUBLIC_API_URL))
    }
}
