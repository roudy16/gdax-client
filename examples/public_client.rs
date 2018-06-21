extern crate chrono;
extern crate curl;
extern crate gdax_client;

use chrono::TimeZone;
use gdax_client::PublicClient;


fn main() {
    let mut public_client = PublicClient::new();

    println!("Products:\n{:?}", public_client.get_products());
    println!("Product Order Book: \n{:?} \n{:?} \n{:?}",
             public_client.get_best_order("BTC-USD"),
             public_client.get_top50_orders("BTC-USD"),
             public_client.get_full_book("BTC-USD"));
    println!("Product Ticker: {:?}", public_client.get_product_ticker("BTC-USD"));
    println!("Latest Trades: {:?}", public_client.get_trades("BTC-USD"));
    println!("Historic Rates: {:?}",
             public_client.get_historic_rates("BTC-USD",
                                              chrono::Utc.ymd(2016, 6, 10).and_hms(0, 0, 0),
                                              chrono::Utc.ymd(2016, 6, 11).and_hms(12, 0, 0),
                                              15 * 60));
    println!("24Hr stats: {:?}", public_client.get_24hr_stats("BTC-USD"));
    println!("Currencies: {:?}", public_client.get_currencies());
    println!("Time: {:?}", public_client.get_time());
}
