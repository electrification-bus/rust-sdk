//! eBus DNS-SD contract.
//!
//! Service types, TXT record construction and broker selection, independent of any
//! mDNS stack. Backends for a particular stack sit behind cargo features.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;
