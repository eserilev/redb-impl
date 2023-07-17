use std::{path::PathBuf, borrow::Borrow};

use redb_impl::{Environment, Config};

fn main() {
    let mut path = PathBuf::new();
    path.push("db");
    
    let config = Config{ database_path: path };
    let db_env = Environment::new(&config).ok().unwrap();

    let open_databases = db_env.create_databases().ok().unwrap();

    let mut rw_transaction = db_env.begin_rw_txn().ok().unwrap();

    let key: [u8; 5] + Borrow = &[1,2,3,4,5];
    let value = vec![3,2,1];

    rw_transaction.put(&open_databases.attesters_db, &key[..].borrow(), value);
}
