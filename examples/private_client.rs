extern crate env_logger;
extern crate gdax_client;
extern crate uuid;

use gdax_client::{NewOrder, PrivateClient, Side, SizeOrFunds};
use uuid::Uuid;

const CB_KEY: &'static str = env!("CB_KEY");
const CB_SECRET: &'static str = env!("CB_SECRET");
const CB_PASSPHRASE: &'static str = env!("CB_PASSPHRASE");


fn main() {
    env_logger::init().unwrap();

    let mut private_client = PrivateClient::new(CB_KEY, CB_SECRET, CB_PASSPHRASE);

    if let Ok(accounts) = private_client.get_accounts() {
        println!("Accounts: {:?}", accounts);

        println!("Account [{}]: {:?}",
                 accounts[0].id,
                 private_client.get_account(accounts[0].id));

        if let Some(btc_account) = accounts.iter().find(|&x| x.currency == "BTC" || x.currency == "ETH") {
            println!("Account History: {:?}", private_client.get_account_history(btc_account.id));
            println!("Account Holds: {:?}", private_client.get_account_holds(btc_account.id));
        }
    }

    //let order = NewOrder::limit(Side::Buy, "BTC-CAD", 1.01, 1.01);
    //println!("Posting limit order: {:?} {:?}", order, private_client.post_order(&order));

    //let order = NewOrder::market(Side::Buy, "BTC-CAD", SizeOrFunds::Funds(10000.));
    //println!("Posting market order: {:?} {:?}", order, private_client.post_order(&order));

    //let order = NewOrder::market(Side::Buy, "BTC-CAD", SizeOrFunds::Size(1000.));
    //println!("Posting market order: {:?} {:?}", order, private_client.post_order(&order));

    //let order = NewOrder::stop(Side::Buy, "BTC-CAD", SizeOrFunds::Size(1.01), 1.01);
    //println!("Posting stop order: {:?} {:?}", order, private_client.post_order(&order));

    println!("All Open Orders: {:?}", private_client.get_orders());

    //println!("Bogus order: {:?}", private_client.get_order(Uuid::new_v4()));

    println!("Cancel bogus order: {:?}", private_client.cancel_order(Uuid::new_v4()));

    //println!("Cancel all orders: {:?}", private_client.cancel_all_orders(None));
}
