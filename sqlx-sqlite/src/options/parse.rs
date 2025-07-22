use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use percent_encoding::{percent_decode_str, percent_encode, AsciiSet};
use url::Url;

use crate::error::Error;
use crate::SqliteConnectOptions;

// https://www.sqlite.org/uri.html

static IN_MEMORY_DB_SEQ: AtomicUsize = AtomicUsize::new(0);

impl SqliteConnectOptions {
    pub(crate) fn from_db_and_params(database: &str, params: Option<&str>) -> Result<Self, Error> {
        let mut options = Self::default();

        if database == ":memory:" {
            options.in_memory = true;
            options.shared_cache = true;
            let seqno = IN_MEMORY_DB_SEQ.fetch_add(1, Ordering::Relaxed);
            options.filename = Cow::Owned(PathBuf::from(format!("file:sqlx-in-memory-{seqno}")));
        } else {
            // % decode to allow for `?` or `#` in the filename
            options.filename = Cow::Owned(
                Path::new(
                    &*percent_decode_str(database)
                        .decode_utf8()
                        .map_err(Error::config)?,
                )
                .to_path_buf(),
            );
        }

        if let Some(params) = params {
            for (key, value) in url::form_urlencoded::parse(params.as_bytes()) {
                match &*key {
                    // The mode query parameter determines if the new database is opened read-only,
                    // read-write, read-write and created if it does not exist, or that the
                    // database is a pure in-memory database that never interacts with disk,
                    // respectively.
                    "mode" => {
                        match &*value {
                            "ro" => {
                                options.read_only = true;
                            }

                            // default
                            "rw" => {}

                            "rwc" => {
                                options.create_if_missing = true;
                            }

                            "memory" => {
                                options.in_memory = true;
                                options.shared_cache = true;
                            }

                            _ => {
                                return Err(Error::Configuration(
                                    format!("unknown value {value:?} for `mode`").into(),
                                ));
                            }
                        }
                    }

                    // The cache query parameter specifies the cache behaviour across multiple
                    // connections to the same database within the process. A shared cache is
                    // essential for persisting data across connections to an in-memory database.
                    "cache" => match &*value {
                        "private" => {
                            options.shared_cache = false;
                        }

                        "shared" => {
                            options.shared_cache = true;
                        }

                        _ => {
                            return Err(Error::Configuration(
                                format!("unknown value {value:?} for `cache`").into(),
                            ));
                        }
                    },

                    "immutable" => match &*value {
                        "true" | "1" => {
                            options.immutable = true;
                        }
                        "false" | "0" => {
                            options.immutable = false;
                        }
                        _ => {
                            return Err(Error::Configuration(
                                format!("unknown value {value:?} for `immutable`").into(),
                            ));
                        }
                    },

                    "vfs" => options.vfs = Some(Cow::Owned(value.into_owned())),

                    // References https://www.sqlite.org/pragma.html
                    "pragma_analysis_limit" | 
                    "pragma_application_id" | 
                    "pragma_auto_vacuum" | 
                    "pragma_automatic_index" | 
                    "pragma_busy_timeout" | 
                    "pragma_cache_size" | 
                    "pragma_cache_spill" | 
                    "pragma_case_sensitive_like" | 
                    "pragma_cell_size_check" | 
                    "pragma_checkpoint_fullfsync" | 
                    "pragma_collation_list" | 
                    "pragma_compile_options" | 
                    "pragma_count_changes" | 
                    "pragma_data_store_directory" | 
                    "pragma_data_version" | 
                    "pragma_database_list" | 
                    "pragma_default_cache_size" | 
                    "pragma_defer_foreign_keys" | 
                    "pragma_empty_result_callbacks" | 
                    "pragma_encoding" | 
                    "pragma_foreign_key_check" | 
                    "pragma_foreign_key_list" | 
                    "pragma_foreign_keys" | 
                    "pragma_freelist_count" | 
                    "pragma_full_column_names" | 
                    "pragma_fullfsync" | 
                    "pragma_function_list" | 
                    "pragma_hard_heap_limit" | 
                    "pragma_ignore_check_constraints" | 
                    "pragma_incremental_vacuum" | 
                    "pragma_index_info" | 
                    "pragma_index_list" | 
                    "pragma_index_xinfo" | 
                    "pragma_integrity_check" | 
                    "pragma_journal_mode" | 
                    "pragma_journal_size_limit" | 
                    "pragma_legacy_alter_table" | 
                    "pragma_legacy_file_format" | 
                    "pragma_locking_mode" | 
                    "pragma_max_page_count" | 
                    "pragma_mmap_size" | 
                    "pragma_module_list" | 
                    "pragma_optimize" | 
                    "pragma_page_count" | 
                    "pragma_page_size" | 
                    "pragma_parser_trace" | 
                    "pragma_pragma_list" | 
                    "pragma_query_only" | 
                    "pragma_quick_check" | 
                    "pragma_read_uncommitted" | 
                    "pragma_recursive_triggers" | 
                    "pragma_reverse_unordered_selects" | 
                    "pragma_schema_version" | 
                    "pragma_secure_delete" | 
                    "pragma_short_column_names" | 
                    "pragma_shrink_memory" | 
                    "pragma_soft_heap_limit" | 
                    "pragma_stats" | 
                    "pragma_synchronous" | 
                    "pragma_table_info" | 
                    "pragma_table_list" | 
                    "pragma_table_xinfo" | 
                    "pragma_temp_store" | 
                    "pragma_temp_store_directory" | 
                    "pragma_threads" | 
                    "pragma_trusted_schema" | 
                    "pragma_user_version" | 
                    "pragma_vdbe_addoptrace" | 
                    "pragma_vdbe_debug" | 
                    "pragma_vdbe_listing" | 
                    "pragma_vdbe_trace" | 
                    "pragma_wal_autocheckpoint" | 
                    "pragma_wal_checkpoint" | 
                    "pragma_writable_schema" => {
                        options = options.pragma(key.into_owned().replace("pragma_", ""), Cow::Owned(value.into_owned()));
                    },

                    _ => {
                        return Err(Error::Configuration(
                            format!("unknown query parameter `{key}` while parsing connection URL")
                                .into(),
                        ));
                    }
                }
            }
        }

        Ok(options)
    }

    pub(crate) fn build_url(&self) -> Url {
        // https://url.spec.whatwg.org/#path-percent-encode-set
        static PATH_ENCODE_SET: AsciiSet = percent_encoding::CONTROLS
            .add(b' ')
            .add(b'"')
            .add(b'#')
            .add(b'<')
            .add(b'>')
            .add(b'?')
            .add(b'`')
            .add(b'{')
            .add(b'}');

        let filename_encoded = percent_encode(
            self.filename.as_os_str().as_encoded_bytes(),
            &PATH_ENCODE_SET,
        );

        let mut url = Url::parse(&format!("sqlite://{filename_encoded}"))
            .expect("BUG: generated un-parseable URL");

        let mode = match (self.in_memory, self.create_if_missing, self.read_only) {
            (true, _, _) => "memory",
            (false, true, _) => "rwc",
            (false, false, true) => "ro",
            (false, false, false) => "rw",
        };
        url.query_pairs_mut().append_pair("mode", mode);

        let cache = match self.shared_cache {
            true => "shared",
            false => "private",
        };
        url.query_pairs_mut().append_pair("cache", cache);

        if self.immutable {
            url.query_pairs_mut().append_pair("immutable", "true");
        }

        if let Some(vfs) = &self.vfs {
            url.query_pairs_mut().append_pair("vfs", vfs);
        }

        url
    }
}

impl FromStr for SqliteConnectOptions {
    type Err = Error;

    fn from_str(mut url: &str) -> Result<Self, Self::Err> {
        // remove scheme from the URL
        url = url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");

        let mut database_and_params = url.splitn(2, '?');

        let database = database_and_params.next().unwrap_or_default();
        let params = database_and_params.next();

        Self::from_db_and_params(database, params)
    }
}

#[test]
fn test_parse_in_memory() -> Result<(), Error> {
    let options: SqliteConnectOptions = "sqlite::memory:".parse()?;
    assert!(options.in_memory);
    assert!(options.shared_cache);

    let options: SqliteConnectOptions = "sqlite://?mode=memory".parse()?;
    assert!(options.in_memory);
    assert!(options.shared_cache);

    let options: SqliteConnectOptions = "sqlite://:memory:".parse()?;
    assert!(options.in_memory);
    assert!(options.shared_cache);

    let options: SqliteConnectOptions = "sqlite://?mode=memory&cache=private".parse()?;
    assert!(options.in_memory);
    assert!(!options.shared_cache);

    Ok(())
}

#[test]
fn test_parse_read_only() -> Result<(), Error> {
    let options: SqliteConnectOptions = "sqlite://a.db?mode=ro".parse()?;
    assert!(options.read_only);
    assert_eq!(&*options.filename.to_string_lossy(), "a.db");

    Ok(())
}

#[test]
fn test_parse_shared_in_memory() -> Result<(), Error> {
    let options: SqliteConnectOptions = "sqlite://a.db?cache=shared".parse()?;
    assert!(options.shared_cache);
    assert_eq!(&*options.filename.to_string_lossy(), "a.db");

    Ok(())
}

#[test]
fn it_returns_the_parsed_url() -> Result<(), Error> {
    let url = "sqlite://test.db?mode=rw&cache=shared";
    let options: SqliteConnectOptions = url.parse()?;

    let expected_url = Url::parse(url).unwrap();
    assert_eq!(options.build_url(), expected_url);

    Ok(())
}
