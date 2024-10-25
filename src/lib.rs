#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

#[cfg(all(feature = "std", not(feature = "alloc")))]
extern crate std;

/// Value log implementation for concurrent environment.
pub mod sync;

/// Value log implementation for single-threaded environment.
pub mod unsync;

mod log;
pub use log::*;

#[cfg(test)]
#[macro_use]
pub(crate) mod tests;

/// Options for configuring the value log.
pub mod options;
pub use options::Builder;

#[doc(inline)]
pub use dbutils as utils;
#[doc(inline)]
pub use dbutils::buffer::VacantBuffer;
pub use dbutils::checksum;

/// Error types.
pub mod error;

dbutils::builder!(
  /// The value builder for building a value in place when inserting into the value log.
  pub ValueBuilder;
);
