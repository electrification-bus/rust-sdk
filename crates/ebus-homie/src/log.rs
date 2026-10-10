//! Diagnostics in the Python SDK's `reason=camelCase,key=value` form.
//!
//! With the `log` feature the macros emit through the `log` crate; without it
//! they expand to code that never runs, so arguments are type-checked but not
//! evaluated. Use them by path:
//!
//! ```text
//! crate::log::debug!("implicitLinkUp");
//! crate::log::warn!("setDropped", topic = topic);
//! ```
//!
//! The second line logs `reason=setDropped,topic={topic}`.

// Not every macro has a caller in every build.
#![allow(unused_macros, unused_imports)]

/// Emit `reason={reason}[,{key}={value}]*` at `level` (a `log` macro name).
macro_rules! emit {
    ($level:ident, $reason:literal $(, $key:ident = $value:expr)* $(,)?) => {{
        #[cfg(feature = "log")]
        ::log::$level!(
            concat!("reason=", $reason $(, ",", stringify!($key), "={}")*)
            $(, $value)*
        );
        #[cfg(not(feature = "log"))]
        if false {
            $(let _ = &$value;)*
        }
    }};
}

/// `reason=...` at debug level.
macro_rules! debug {
    ($($arg:tt)+) => { $crate::log::emit!(debug, $($arg)+) };
}

/// `reason=...` at info level.
macro_rules! info {
    ($($arg:tt)+) => { $crate::log::emit!(info, $($arg)+) };
}

/// `reason=...` at warn level. Exported as `warn`; the macro itself cannot
/// carry that name, which is also a built-in attribute.
macro_rules! warn_ {
    ($($arg:tt)+) => { $crate::log::emit!(warn, $($arg)+) };
}

/// `reason=...` at error level.
macro_rules! error {
    ($($arg:tt)+) => { $crate::log::emit!(error, $($arg)+) };
}

pub(crate) use {debug, emit, error, info, warn_ as warn};

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "log"))]
    #[test]
    fn disabled_macros_do_not_evaluate_arguments() {
        let mut calls = 0;
        let mut count = || {
            calls += 1;
            calls
        };
        crate::log::debug!("plain");
        crate::log::info!("withKey", topic = "a/b");
        crate::log::warn!("twoKeys", device = "d1", attempt = count(),);
        crate::log::error!("oneKey", n = 3);
        assert_eq!(calls, 0);
    }

    #[cfg(feature = "log")]
    #[test]
    fn message_text() {
        use std::sync::Mutex;

        static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

        struct Capture;
        impl ::log::Log for Capture {
            fn enabled(&self, _: &::log::Metadata<'_>) -> bool {
                true
            }
            fn log(&self, record: &::log::Record<'_>) {
                if let Ok(mut lines) = LINES.lock() {
                    lines.push(format!("{} {}", record.level(), record.args()));
                }
            }
            fn flush(&self) {}
        }

        static CAPTURE: Capture = Capture;
        if ::log::set_logger(&CAPTURE).is_ok() {
            ::log::set_max_level(::log::LevelFilter::Trace);
        }
        crate::log::debug!("implicitLinkUp");
        crate::log::warn!("setDropped", topic = "ebus/5/d/n/p/set", attempt = 2);
        let lines = LINES.lock().map(|l| l.clone()).unwrap_or_default();
        assert!(lines.contains(&"DEBUG reason=implicitLinkUp".to_string()));
        assert!(
            lines.contains(&"WARN reason=setDropped,topic=ebus/5/d/n/p/set,attempt=2".to_string())
        );
    }
}
