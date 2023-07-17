use std::{
    borrow::{Borrow, Cow},
    marker::PhantomData,
    path::PathBuf,
};

use redb::{ReadableTable, TableDefinition};

const INDEXED_ATTESTATION_DB: &str = "indexed_attestations";
const INDEXED_ATTESTATION_ID_DB: &str = "indexed_attestation_ids";
const ATTESTERS_DB: &str = "attesters";
const ATTESTERS_MAX_TARGETS_DB: &str = "attesters_max_targets";
const MIN_TARGETS_DB: &str = "min_targets";
const MAX_TARGETS_DB: &str = "max_targets";
const CURRENT_EPOCHS_DB: &str = "current_epochs";
const PROPOSERS_DB: &str = "proposers";
const METADATA_DB: &str = "metadata";

const BASE_DB: &str = "base_db";

pub type Key<'a> = Cow<'a, [u8]>;
pub type Value<'a> = Cow<'a, [u8]>;

pub struct Config {
    pub database_path: PathBuf,
}

pub struct OpenDatabases<'env> {
    pub indexed_attestation_db: Database<'env>,
    pub indexed_attestation_id_db: Database<'env>,
    pub attesters_db: Database<'env>,
    pub attesters_max_targets_db: Database<'env>,
    pub min_targets_db: Database<'env>,
    pub max_targets_db: Database<'env>,
    pub current_epochs_db: Database<'env>,
    pub proposers_db: Database<'env>,
    pub metadata_db: Database<'env>,
}

pub enum Error {
    DatabaseRedbError(),
}

pub struct Database<'env> {
    table: &'env str,
    _phantom: PhantomData<&'env ()>,
}

pub struct RwTransaction<'env> {
    txn: PhantomData<&'env ()>,
    _phantom: PhantomData<&'env ()>,
}

pub struct Environment {
    env: PathBuf,
}

pub struct Cursor<'env> {
    db: &'env Database<'env>,
    current_key: Cow<'env, [u8]>,
    _phantom: PhantomData<&'env ()>,
}

impl Environment {
    pub fn new(config: &Config) -> Result<Environment, Error> {
        let env = config.database_path.clone();
        Ok(Environment { env })
    }

    pub fn create_databases(&self) -> Result<OpenDatabases, Error> {
        let indexed_attestation_db = self.create_table(INDEXED_ATTESTATION_DB);
        let indexed_attestation_id_db = self.create_table(INDEXED_ATTESTATION_ID_DB);
        let attesters_db = self.create_table(ATTESTERS_DB);
        let attesters_max_targets_db = self.create_table(ATTESTERS_MAX_TARGETS_DB);
        let min_targets_db = self.create_table(MIN_TARGETS_DB);
        let max_targets_db = self.create_table(MAX_TARGETS_DB);
        let current_epochs_db = self.create_table(CURRENT_EPOCHS_DB);
        let proposers_db = self.create_table(PROPOSERS_DB);
        let metadata_db = self.create_table(METADATA_DB);

        Ok(OpenDatabases {
            indexed_attestation_db: indexed_attestation_db,
            indexed_attestation_id_db: indexed_attestation_id_db,
            attesters_db: attesters_db,
            attesters_max_targets_db: attesters_max_targets_db,
            min_targets_db: min_targets_db,
            max_targets_db: max_targets_db,
            current_epochs_db: current_epochs_db,
            proposers_db: proposers_db,
            metadata_db: metadata_db,
        })
    }

    pub fn create_table<'env>(&self, table_name: &'env str) -> Database<'env> {
        Database {
            table: table_name,
            _phantom: PhantomData,
        }
    }

    pub fn begin_rw_txn(&self) -> Result<RwTransaction, Error> {
        Ok(RwTransaction {
            txn: PhantomData,
            _phantom: PhantomData,
        })
    }
}

pub fn process<K: AsRef<[u8]> + ?Sized>(val: impl Borrow<K>) -> &[u8] {
    let x = val.borrow();
    x
}

impl<'env> RwTransaction<'env> {
    pub fn get<K: AsRef<[u8]> + ?Sized>(
        &'env self,
        db: &Database<'env>,
        key: &K,
    ) -> Result<Option<Cow<'_, [u8]>>, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(BASE_DB);
        let database = redb::Database::open(db.table).unwrap();
        let tx = database.begin_read().unwrap();
        let table = tx.open_table(table_definition).unwrap();
       
        let value = table.get(key.clone()).unwrap().unwrap().value().to_vec();
        Ok(Some(Cow::from(value)))
    }

    pub fn put<
        K: AsRef<[u8]> + std::borrow::Borrow<&'env [u8]>,
        V: AsRef<[u8]> + std::borrow::Borrow<&'env [u8]>,
    >(
        &mut self,
        db: &Database,
        key: K,
        value: V,
    ) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(BASE_DB);
        let database = redb::Database::open(db.table).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_definition).unwrap();
            table.insert(key, value).unwrap();
        }
        Ok(())
    }

    pub fn del<K: AsRef<[u8]> + std::borrow::Borrow<&'env [u8]>>(
        &mut self,
        db: &Database,
        key: K,
    ) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(BASE_DB);
        let database = redb::Database::open(db.table).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_definition).unwrap();
            table.remove(key).unwrap();
        }
        Ok(())
    }

    pub fn cursor<'a>(&'a mut self, db: &Database) -> Result<Cursor<'a>, Error> {
        Ok(Cursor {
            db: db.clone(),
            current_key: todo!(),
            _phantom: PhantomData,
        })
    }

    pub fn commit(self) -> Result<(), Error> {
        todo!()
    }
}

impl<'env> Cursor<'env> {
    pub fn first_key(&mut self) -> Result<Option<Key>, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_read().unwrap();
        let first = tx
            .open_table(table_definition)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .map(|x| x.map(|(key, _)| (key.value()).to_vec()));

        if let Some(owned_key) = first {
            let owned_key = owned_key.unwrap();
            self.current_key = Cow::from(owned_key);
            Ok(Some(self.current_key.clone()))
        } else {
            panic!()
        }
    }

    pub fn last_key(&mut self) -> Result<Option<Key<'env>>, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_read().unwrap();
        let last = tx
            .open_table(table_definition)
            .unwrap()
            .iter()
            .unwrap()
            .rev()
            .next()
            .map(|x| x.map(|(key, _)| (key.value()).to_vec()));

        if let Some(owned_key) = last {
            let owned_key = owned_key.unwrap();
            self.current_key = Cow::from(owned_key);
            Ok(Some(self.current_key.clone()))
        } else {
            panic!()
        }
    }

    pub fn next_key(&mut self) -> Result<Option<Key<'env>>, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_read().unwrap();
        let range: std::ops::RangeFrom<&[u8]> = &self.current_key..;
        let next = tx
            .open_table(table_definition)
            .unwrap()
            .range(range)
            .unwrap()
            .next()
            .map(|x| x.map(|(key, _)| (key.value()).to_vec()));

        if let Some(owned_key) = next {
            let owned_key = owned_key.unwrap();
            self.current_key = Cow::from(owned_key);
            Ok(Some(self.current_key.clone()))
        } else {
            panic!()
        }
    }

    pub fn get_current(&mut self) -> Result<Option<(Key<'env>, Value<'env>)>, Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_read().unwrap();
        let table = tx.open_table(table_definition).unwrap();
        let value = table
            .get(self.current_key.as_ref())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        Ok(Some((self.current_key.clone(), Cow::from(value))))
    }

    pub fn delete_current(&mut self) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_definition).unwrap();
            table.remove(self.current_key.as_ref()).unwrap();
        }
        Ok(())
    }

    pub fn put<K: AsRef<[u8]> + Borrow<&'env [u8]>, V: AsRef<[u8]> + Borrow<&'env [u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), Error> {
        let table_definition: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(self.db.table);
        let database = redb::Database::open(self.db.table).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_definition).unwrap();
            table.insert(key, value).unwrap();
            // set cursor current key to key
        }
        Ok(())
    }
}
