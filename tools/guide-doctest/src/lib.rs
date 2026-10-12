//! Compiles the code in `doc/python-sdk.md` and the root `README.md` as
//! doctests. Neither file is inside a published package, so this unpublished
//! crate holds them.

#[cfg(doctest)]
#[doc = include_str!("../../../doc/python-sdk.md")]
struct PythonSdkGuide;

#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct Readme;
