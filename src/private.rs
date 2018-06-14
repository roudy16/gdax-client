use base64;
use chrono::{DateTime, Utc};
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;

use curl::easy::{Easy, List};

use futures::{Async, Poll, Stream};

use serde::{self, Deserialize, Serialize};
use serde_json::{de, ser};
use std::ops::Deref;
use time::get_time;
use uuid::Uuid;

use std::fmt;

use super::Error;
use super::ApiError;
use super::Side;

const PRIVATE_API_URL: &'static str = "https://api.gdax.com";

pub struct Client {
    public_client: super::public::Client,
    curl: Easy,
    key: String,
    secret: String,
    passphrase: String
}

#[derive(Deserialize, Debug)]
pub struct Account {
    pub id: Uuid,
    pub balance: f64,
    pub hold: f64,
    pub available: f64,
    pub currency: String
}

pub type Ledger = Vec<LedgerEntry>;

#[derive(Deserialize, Debug)]
pub struct LedgerEntry {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub amount: f64,
    pub balance: f64,
    // #[serde(rename = "type")]
    pub entry_type: EntryType,
    pub details: Option<EntryDetails>
}

#[derive(Deserialize, Debug)]
pub struct EntryDetails {
    pub order_id: Option<Uuid>,
    pub trade_id: Option<u64>,
    pub product_id: Option<String>,
    pub transfer_id: Option<Uuid>,
    pub transfer_type: Option<String>
}

#[derive(Debug)]
pub enum EntryType {
    Fee,
    Match,
    Transfer
}

// We manually implement Deserialize for EntryType here
// because the default encoding/decoding scheme that derive
// gives us isn't the straightforward mapping unfortunately
impl<'de> serde::Deserialize<'de> for EntryType {
    fn deserialize<D>(deserializer: D) -> Result<EntryType, D::Error>
        where D: serde::Deserializer<'de> {

        struct EntryTypeVisitor;
        impl<'a> serde::de::Visitor<'a> for EntryTypeVisitor {
            type Value = EntryType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                unimplemented!()
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                match &*v.to_lowercase() {
                    "fee" => Ok(EntryType::Fee),
                    "match" => Ok(EntryType::Match),
                    "transfer" => Ok(EntryType::Transfer),
                    _ => Err(E::invalid_value(serde::de::Unexpected::Str("Invalid entry type"), &self))
                }
            }
        }
        deserializer.deserialize_identifier(EntryTypeVisitor)
    }
}

#[derive(Deserialize, Debug)]
pub struct Hold {
    pub id: Uuid,
    pub account_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub amount: f64,
    // #[serde(rename = "type")]
    pub hold_type: HoldType,
    // #[serde(rename = "ref")]
    pub ref_id: Uuid
}

#[derive(Debug)]
pub enum HoldType {
    Order,
    Transfer
}

// We manually implement Deserialize for HoldType here
// because the default encoding/decoding scheme that derive
// gives us isn't the straightforward mapping unfortunately
impl<'de> serde::Deserialize<'de> for HoldType {
    fn deserialize<D>(deserializer: D) -> Result<HoldType, D::Error>
        where D: serde::Deserializer<'de> {

        struct HoldTypeVisitor;
        impl<'a> serde::de::Visitor<'a> for HoldTypeVisitor {
            type Value = HoldType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                unimplemented!()
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                match &*v.to_lowercase() {
                    "order" => Ok(HoldType::Order),
                    "transfer" => Ok(HoldType::Transfer),
                    _ => Err(E::invalid_value(serde::de::Unexpected::Str("Invalid hold type"), &self))
                }
            }
        }
        deserializer.deserialize_identifier(HoldTypeVisitor)
    }
}

pub type OrderId = Uuid;

#[derive(Clone, Copy, Debug)]
pub enum SizeOrFunds {
    Size(f64),
    Funds(f64)
}

#[derive(Debug)]
pub enum NewOrder {
    Limit {
        side: Side,
        product_id: String,
        price: f64,
        size: f64
    },
    Market {
        side: Side,
        product_id: String,
        size_or_funds: SizeOrFunds,
    },
    Stop {
        side: Side,
        product_id: String,
        price: f64,
        size_or_funds: SizeOrFunds
    }
}

impl NewOrder {
    pub fn limit(side: Side, product_id: &str, size: f64, price: f64) -> NewOrder {
        NewOrder::Limit {
            side: side,
            product_id: product_id.to_owned(),
            price: price,
            size: size
        }
    }

    pub fn market(side: Side, product_id: &str, size_or_funds: SizeOrFunds) -> NewOrder {
        NewOrder::Market {
            side: side,
            product_id: product_id.to_owned(),
            size_or_funds: size_or_funds
        }
    }

    pub fn stop(side: Side, product_id: &str, size_or_funds: SizeOrFunds, price: f64) -> NewOrder {
        NewOrder::Stop {
            side: side,
            product_id: product_id.to_owned(),
            size_or_funds: size_or_funds,
            price: price
        }
    }
}

// We manually implement Serialize for NewOrder since
// each variant needs to be encoded slightly differently
impl Serialize for NewOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer
    {
        match *self {
            NewOrder::Limit { side, ref product_id, price, size } => {
                // We create a struct representing the JSON
                // and have Serialize auto derived for that
                #[derive(Serialize)]
                struct LimitOrder<'a> {
                    // #[serde(rename = "type")]
                    t: &'static str,
                    side: Side,
                    product_id: &'a String,
                    price: f64,
                    size: f64
                }
                LimitOrder {
                    t: "limit",
                    side: side,
                    product_id: &product_id,
                    price: price,
                    size: size
                }.serialize(serializer)
            }

            NewOrder::Market { side, ref product_id, size_or_funds: SizeOrFunds::Size(size) } => {
                #[derive(Serialize)]
                struct MarketOrder<'a> {
                    // #[serde(rename = "type")]
                    t: &'static str,
                    side: Side,
                    product_id: &'a String,
                    size: f64
                }
                MarketOrder {
                    t: "market",
                    side: side,
                    product_id: &product_id,
                    size: size
                }.serialize(serializer)
            }

            NewOrder::Market { side, ref product_id, size_or_funds: SizeOrFunds::Funds(funds) } => {
                #[derive(Serialize)]
                struct MarketOrder<'a> {
                    // #[serde(rename = "type")]
                    t: &'static str,
                    side: Side,
                    product_id: &'a String,
                    funds: f64
                }
                MarketOrder {
                    t: "market",
                    side: side,
                    product_id: &product_id,
                    funds: funds
                }.serialize(serializer)
            }

            NewOrder::Stop { side, ref product_id, price, size_or_funds: SizeOrFunds::Size(size) } => {
                #[derive(Serialize)]
                struct StopOrder<'a> {
                    // #[serde(rename = "type")]
                    t: &'static str,
                    side: Side,
                    product_id: &'a String,
                    price: f64,
                    size: f64
                }
                StopOrder {
                    t: "stop",
                    side: side,
                    product_id: &product_id,
                    price: price,
                    size: size
                }.serialize(serializer)
            }

            NewOrder::Stop { side, ref product_id, price, size_or_funds: SizeOrFunds::Funds(funds) } => {
                #[derive(Serialize)]
                struct StopOrder<'a> {
                    // #[serde(rename = "type")]
                    t: &'static str,
                    side: Side,
                    product_id: &'a String,
                    price: f64,
                    funds: f64
                }
                StopOrder {
                    t: "stop",
                    side: side,
                    product_id: &product_id,
                    price: price,
                    funds: funds
                }.serialize(serializer)
            }
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct OpenOrder {
    pub id: OrderId,
    pub size: f64,
    pub price: f64,
    pub product_id: String,
    pub status: String,
    pub filled_size: f64,
    pub executed_value: f64,
    pub fill_fees: f64,
    pub settled: bool,
    pub side: Side,
    pub created_at: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct Order {
    pub id: OrderId,
    pub size: f64,
    pub price: f64,
    pub done_reason: Option<String>,
    pub status: String,
    pub settled: bool,
    pub filled_size: f64,
    pub executed_value: f64,
    pub product_id: String,
    pub fill_fees: f64,
    pub side: Side,
    pub created_at: DateTime<Utc>,
    pub done_at: Option<DateTime<Utc>>
}

impl Client {
    pub fn new(key: &str, secret: &str, passphrase: &str) -> Client {
        Client {
            public_client: super::public::Client::new(),
            curl: Easy::new(),
            key: key.to_owned(),
            secret: secret.to_owned(),
            passphrase: passphrase.to_owned()
        }
    }

    fn signature(&self, path: &str, body: &str, timestamp: &str, method: &str)
        -> Result<String, Error> {

        let key = base64::decode(&self.secret)?;
        let what = format!("{}{}{}{}",
                           timestamp,
                           method.to_uppercase(),
                           path,
                           body);

        let mut hmac = Hmac::new(Sha256::new(), &key);
        hmac.input(what.as_bytes());

        Ok(base64::encode(hmac.result().code()))
    }

    fn get_headers(&self, path: &str, body: &str, method: &str) -> Result<List, Error> {
        let timestamp = get_time().sec.to_string();
        let signature = self.signature(path, body, &timestamp, method)?;

        let mut headers = List::new();
        headers.append("Accept: application/json").unwrap();
        headers.append("User-Agent: rust-gdax-client/1.2.0").unwrap();
        headers.append(["CB-ACCESS-KEY: ", self.key.as_str()].join("").as_str()).unwrap();
        headers.append(["CB-ACCESS-SIGN: ", signature.as_str()].join("").as_str()).unwrap();
        headers.append(["CB-ACCESS-PASSPHRASE: ", self.passphrase.as_str()].join("").as_str()).unwrap();
        headers.append(["CB-ACCESS-TIMESTAMP: ", timestamp.as_str()].join("").as_str()).unwrap();

        Ok(headers)
    }

    fn get_and_decode<T>(&mut self, path: &str) -> Result<T, Error>
        where for<'de> T: Deserialize<'de>
    {
        let headers: List = self.get_headers(path, "", "GET")?;
        let url = format!("{}{}", PRIVATE_API_URL, path);
        self.curl.url(url.as_str()).unwrap();
        self.curl.http_headers(headers).unwrap();

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
            return Ok(de::from_reader(&mut buf.as_slice())?);
        }
    }

    fn post_and_decode<T>(&mut self, path: &str, body: &str) -> Result<T, Error>
        where for<'de> T: Deserialize<'de>
    {
        let headers: List = self.get_headers(path, body, "POST")?;
        let url = format!("{}{}", PRIVATE_API_URL, path);
        self.curl.url(url.as_str()).unwrap();
        self.curl.http_headers(headers).unwrap();

        let mut buf = Vec::new();

        {
            let mut t = self.curl.transfer();
            t.write_function(|data| {
                buf.extend_from_slice(data);
                Ok(data.len())
            }).unwrap();
            t.perform().unwrap();
        }

        // TODO success codes can be more than just 200
        if self.curl.response_code().unwrap() != 200 {
            return Err(Error::Api(ApiError{ message: String::from_utf8(buf).unwrap()}));
        } else {
            return Ok(de::from_reader(&mut buf.as_slice())?);
        }
    }

    fn delete_and_decode<T>(&mut self, path: &str) -> Result<T, Error>
        where for<'de> T: Deserialize<'de>
    {
        let headers: List = self.get_headers(path, "", "DELETE")?;
        let url = format!("{}{}", PRIVATE_API_URL, path);
        self.curl.url(url.as_str()).unwrap();
        self.curl.http_headers(headers).unwrap();

        let mut buf = Vec::new();

        {
            let mut t = self.curl.transfer();
            t.write_function(|data| {
                buf.extend_from_slice(data);
                Ok(data.len())
            }).unwrap();
            t.perform().unwrap();
        }

        // TODO success codes can be more than just 200
        if self.curl.response_code().unwrap() != 200 {
            return Err(Error::Api(ApiError{ message: String::from_utf8(buf).unwrap()}));
        } else {
            return Ok(de::from_reader(&mut buf.as_slice())?);
        }
    }

    pub fn get_accounts(&mut self) -> Result<Vec<Account>, Error> {
        self.get_and_decode("/accounts")
    }

    pub fn get_account(&mut self, id: Uuid) -> Result<Account, Error> {
        self.get_and_decode(&format!("/accounts/{}", id))
    }

    pub fn get_account_history(&mut self, id: Uuid) -> Result<Ledger, Error> {
        self.get_and_decode(&format!("/accounts/{}/ledger", id))
    }

    pub fn get_account_holds(&mut self, id: Uuid) -> Result<Vec<Hold>, Error> {
        self.get_and_decode(&format!("/accounts/{}/holds", id))
    }

    pub fn post_order(&mut self, order: &NewOrder) -> Result<OrderId, Error> {
        #[derive(Deserialize)]
        struct NewOrderResult { id: OrderId }

        let body = ser::to_string(order)?;
        Ok(self.post_and_decode::<NewOrderResult>("/orders", &body)?.id)
    }

    pub fn cancel_order(&mut self, order_id: OrderId) -> Result<OrderId, Error> {
        Ok(self.delete_and_decode::<Vec<OrderId>>(&format!("/orders/{}", order_id))?[0])
    }

    pub fn cancel_all_orders(&mut self, product_id: Option<&str>) -> Result<Vec<OrderId>, Error> {
        if let Some(product_id) = product_id {
            self.delete_and_decode(&format!("/orders?product_id={}", product_id))
        } else {
            self.delete_and_decode("/orders")
        }
    }

    pub fn get_orders_with_status(&mut self,
                                  open: bool,
                                  pending: bool,
                                  active: bool)
        -> Result<Vec<OpenOrder>, Error>
    {
        let status = [open, pending, active].iter()
                                            .zip(["status=open", "status=pending", "status=active"].iter())
                                            .filter(|&(&flag, _)| flag)
                                            .map(|(_, &s)| s)
                                            .collect::<Vec<_>>()
                                            .join("&");
        self.get_and_decode(&format!("/orders?{}", status))
    }

    pub fn get_orders(&mut self) -> Result<Vec<OpenOrder>, Error> {
        self.get_orders_with_status(true, true, true)
    }

    pub fn get_order(&mut self, order_id: OrderId) -> Result<Order, Error> {
        self.get_and_decode(&format!("/orders/{}", order_id))
    }
}

impl Deref for Client {
    type Target = super::public::Client;

    fn deref(&self) -> &Self::Target {
        &self.public_client
    }
}
