//! A lightweight async prelude.
//!
//! This crate is a subset of [futures] that compiles an order of magnitude faster, fixes minor
//! warts in its API, fills in some obvious gaps, and removes almost all unsafe code from it.
//!
//! In short, this crate aims to be more enjoyable than [futures] but still fully compatible with
//! it.
//!
//! [futures]: https://docs.rs/futures
//!
//! # Examples
//!
//! ```no_run
//! use futures_lite::*;
//!
//! fn main() {
//!     future::block_on(async {
//!         println!("Hello world!");
//!     })
//! }
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

#![cfg_attr(not(feature = "std"), no_std)]

#[doc(no_inline)]
pub use core::future::Future;

#[doc(no_inline)]
pub use futures_core::Stream;

#[cfg(feature = "std")]
#[doc(no_inline)]
pub use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};

#[doc(no_inline)]
pub use crate::future::FutureExt;

#[cfg(feature = "std")]
#[doc(no_inline)]
pub use crate::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[doc(no_inline)]
pub use crate::stream::StreamExt;

pub mod future;
pub mod stream;

#[cfg(feature = "std")]
pub mod io;

pub use futures_micro::{ready, pin};
