//! Topic names and topic filters, per MQTT 3.1.1 section 4.7.
//!
//! Levels are separated by `/`. In a filter, `+` matches exactly one level
//! (which may be empty) and `#` matches its parent level and any number of
//! levels below it. A filter whose first character is a wildcard does not match
//! a topic name that starts with `$` (section 4.7.2).
//!
//! Every function here returns `false` for an invalid topic name or filter.

/// Maximum length in bytes of a topic name or filter: the two-byte length
/// prefix of an MQTT UTF-8 string (section 1.5.3).
pub const MAX_LEN: usize = 65_535;

/// Checks the rules a topic name and a topic filter share: at least one
/// character, at most [`MAX_LEN`] bytes, and no U+0000 (section 4.7.3).
fn is_valid_string(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_LEN && !s.contains('\0')
}

/// True when `f` is a valid topic filter.
///
/// `+` must occupy a whole level, and `#` must occupy a whole level and be the
/// last one (sections 4.7.1.2 and 4.7.1.3).
pub fn is_valid_filter(f: &str) -> bool {
    if !is_valid_string(f) {
        return false;
    }
    let mut levels = f.split('/').peekable();
    while let Some(level) = levels.next() {
        let wildcard_ok = match level {
            "#" => levels.peek().is_none(),
            "+" => true,
            _ => !level.contains(['+', '#']),
        };
        if !wildcard_ok {
            return false;
        }
    }
    true
}

/// True when `t` is a valid topic name: a valid string with no wildcard
/// characters (section 4.7.1.1).
pub fn is_valid_topic(t: &str) -> bool {
    is_valid_string(t) && !t.contains(['+', '#'])
}

/// True when a filter's first level is a wildcard and the topic starts with
/// `$`, which section 4.7.2 forbids from matching.
fn leading_wildcard_vs_dollar(filter_first: &str, topic_first: &str) -> bool {
    (filter_first == "+" || filter_first == "#") && topic_first.starts_with('$')
}

/// True when topic name `topic` matches topic filter `filter` (section 4.7).
///
/// `sport/#` matches `sport`, `+/+` matches `/finance`, and `#` does not match
/// `$SYS/broker`. Returns `false` when either argument is invalid.
pub fn matches(filter: &str, topic: &str) -> bool {
    if !is_valid_filter(filter) || !is_valid_topic(topic) {
        return false;
    }
    let mut fl = filter.split('/');
    let mut tl = topic.split('/');
    let mut first = true;
    loop {
        match (fl.next(), tl.next()) {
            (Some(f), t) if first && leading_wildcard_vs_dollar(f, t.unwrap_or("")) => {
                return false;
            }
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(f), Some(t)) => {
                if f != t {
                    return false;
                }
            }
            (None, None) => return true,
            (Some(_), None) | (None, Some(_)) => return false,
        }
        first = false;
    }
}

/// True when some topic name matches both filters `a` and `b`.
///
/// This is ebus-mqtt-client's `_filters_overlap`: levels are compared pairwise;
/// a `#` on either side overlaps whatever remains, `+` overlaps any level, and a
/// wildcard first level does not overlap a first level that starts with `$`.
/// When one filter runs out first, they overlap only if the longer one has
/// exactly one more level and it is `#`. Returns `false` when either argument
/// is invalid.
///
/// For every other pair of valid filters the result is exact. `+` and `/#` are
/// reported as overlapping although the only topic both match is the empty
/// string, which is not a valid topic name.
pub fn overlap(a: &str, b: &str) -> bool {
    if !is_valid_filter(a) || !is_valid_filter(b) {
        return false;
    }
    let mut la = a.split('/');
    let mut lb = b.split('/');
    let mut first = true;
    loop {
        match (la.next(), lb.next()) {
            (Some(x), Some(y)) => {
                let wild_x = x == "#" || x == "+";
                let wild_y = y == "#" || y == "+";
                if wild_x || wild_y {
                    if first && (x.starts_with('$') || y.starts_with('$')) {
                        return false;
                    }
                    if x == "#" || y == "#" {
                        return true;
                    }
                } else if x != y {
                    return false;
                }
            }
            (None, None) => return true,
            (Some(rest), None) => return rest == "#" && la.next().is_none(),
            (None, Some(rest)) => return rest == "#" && lb.next().is_none(),
        }
        first = false;
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use std::{format, string::String, vec, vec::Vec};

    #[test]
    fn filter_validity_section_4_7_1() {
        for ok in [
            "#",
            "+",
            "/",
            "//",
            "sport/tennis/player1",
            "sport/tennis/#",
            "sport/tennis/+",
            "+/tennis/#",
            "sport/+/player1",
            "/+",
            "+/+",
            "$SYS/#",
            "$SYS/monitor/+",
            "/finance",
            "a/",
            " ",
            "caf\u{e9}/\u{1f50c}",
        ] {
            assert!(is_valid_filter(ok), "{ok:?}");
        }
        for bad in [
            "",
            "sport/tennis#",
            "sport/tennis/#/ranking",
            "sport+",
            "sport/+tennis",
            "#/",
            "##",
            "++",
            "a/#/b",
            "a/b#",
            "+#",
            "a\0b",
            "\0",
        ] {
            assert!(!is_valid_filter(bad), "{bad:?}");
        }
    }

    #[test]
    fn topic_validity_section_4_7_1_1() {
        for ok in [
            "a",
            "/",
            "/finance",
            "$SYS/broker",
            "a//b",
            " ",
            "\u{1f50c}",
        ] {
            assert!(is_valid_topic(ok), "{ok:?}");
        }
        for bad in ["", "a/+", "#", "a/#", "sport+", "a\0", "a#b"] {
            assert!(!is_valid_topic(bad), "{bad:?}");
        }
    }

    #[test]
    fn length_limit() {
        let max = "a".repeat(MAX_LEN);
        let over = "a".repeat(MAX_LEN + 1);
        assert!(is_valid_topic(&max));
        assert!(is_valid_filter(&max));
        assert!(!is_valid_topic(&over));
        assert!(!is_valid_filter(&over));
        assert!(matches(&max, &max));
        assert!(!matches(&over, &over));
        assert!(overlap(&max, &max));
        assert!(!overlap(&over, &over));
    }

    #[test]
    fn matches_section_4_7_examples() {
        let yes = [
            ("sport/tennis/player1/#", "sport/tennis/player1"),
            ("sport/tennis/player1/#", "sport/tennis/player1/ranking"),
            (
                "sport/tennis/player1/#",
                "sport/tennis/player1/score/wimbledon",
            ),
            ("sport/#", "sport"),
            ("#", "sport"),
            ("#", "/"),
            ("#", "a/b/c"),
            ("sport/tennis/+", "sport/tennis/player1"),
            ("sport/tennis/+", "sport/tennis/player2"),
            ("sport/+", "sport/"),
            ("+/+", "/finance"),
            ("/+", "/finance"),
            ("+/tennis/#", "sport/tennis/player1"),
            ("$SYS/#", "$SYS/monitor/Clients"),
            ("$SYS/#", "$SYS"),
            ("$SYS/monitor/+", "$SYS/monitor/Clients"),
            ("a/$x/+", "a/$x/b"),
            ("a/+", "a/$b"),
            ("a/#", "a/$b/c"),
            ("ebus/5/+/$state", "ebus/5/dev-1/$state"),
            ("+/5/+/$state", "ebus/5/dev-1/$state"),
        ];
        for (f, t) in yes {
            assert!(matches(f, t), "{f:?} {t:?}");
        }
        let no = [
            ("sport/tennis/+", "sport/tennis/player1/ranking"),
            ("sport/+", "sport"),
            ("+", "/finance"),
            ("sport/tennis/player1", "sport/tennis/player2"),
            ("sport/tennis", "sport/tennis/player1"),
            ("sport/tennis/player1", "sport/tennis"),
            ("#", "$SYS/monitor/Clients"),
            ("#", "$SYS"),
            ("+", "$SYS"),
            ("+/monitor/Clients", "$SYS/monitor/Clients"),
            ("+/#", "$SYS/x"),
            ("Sport", "sport"),
            ("a/b", "a/b/"),
            ("a/b/", "a/b"),
            // Invalid arguments.
            ("sport/tennis#", "sport/tennis"),
            ("", ""),
            ("#", ""),
            ("a", "a/+"),
            ("a/+", "a/+"),
        ];
        for (f, t) in no {
            assert!(!matches(f, t), "{f:?} {t:?}");
        }
    }

    #[test]
    fn overlap_examples() {
        let yes = [
            ("a/b", "a/b"),
            ("a/+", "a/b"),
            ("a/#", "a"),
            ("a", "a/#"),
            ("a/+", "a/b/#"),
            ("+", "#"),
            ("#", "#"),
            ("+/+", "/#"),
            ("$SYS/#", "$SYS/+"),
            ("$SYS/#", "$SYS"),
            ("+", "/#"),
            ("a/+", "a//#"),
            ("ebus/5/+/$state", "ebus/5/dev/$state"),
            ("ebus/5/+/$description", "+/5/+/$description"),
            ("ebus/5/dev/+/+", "ebus/5/dev/node/+"),
            ("ebus/5/dev/+/+/$target", "ebus/5/dev/#"),
        ];
        for (a, b) in yes {
            assert!(overlap(a, b), "{a:?} {b:?}");
            assert!(overlap(b, a), "{b:?} {a:?}");
        }
        let no = [
            ("a", "b"),
            ("a", "a/+"),
            ("a/+", "a"),
            ("a/b", "a/b/c"),
            ("+", "$SYS"),
            ("#", "$SYS/#"),
            ("+/x", "$SYS/+"),
            ("ebus/5/+/$state", "ebus/5/+/$description"),
            ("ebus/5/dev/+/+", "ebus/5/dev/+/+/$target"),
            ("a/#", "b/#"),
            // Invalid arguments.
            ("a#", "a#"),
            ("", "#"),
            ("#", ""),
            ("a/#/b", "#"),
        ];
        for (a, b) in no {
            assert!(!overlap(a, b), "{a:?} {b:?}");
            assert!(!overlap(b, a), "{b:?} {a:?}");
        }
    }

    /// Independent recursive reading of section 4.7, used as an oracle.
    fn oracle_matches(filter: &[&str], topic: &[&str], first: bool) -> bool {
        if first && topic.first().is_some_and(|t| t.starts_with('$')) {
            if let Some(f) = filter.first() {
                if *f == "+" || *f == "#" {
                    return false;
                }
            }
        }
        match (filter.split_first(), topic.split_first()) {
            (None, None) => true,
            (Some((&"#", rest)), _) => rest.is_empty(),
            (Some((f, frest)), Some((t, trest))) => {
                (*f == "+" || f == t) && oracle_matches(frest, trest, false)
            }
            _ => false,
        }
    }

    fn sequences(alphabet: &[&'static str], max_len: usize) -> Vec<Vec<&'static str>> {
        let mut out: Vec<Vec<&'static str>> = vec![vec![]];
        let mut frontier = out.clone();
        for _ in 0..max_len {
            let mut next = Vec::new();
            for seq in &frontier {
                for sym in alphabet {
                    let mut s = seq.clone();
                    s.push(*sym);
                    next.push(s);
                }
            }
            out.extend(next.iter().cloned());
            frontier = next;
        }
        out
    }

    /// Filters of up to three levels over literals, `+` and a final `#`,
    /// including invalid strings (the empty filter).
    fn generated_filters() -> Vec<String> {
        let mut out = Vec::new();
        for seq in sequences(&["a", "b", "$s", "", "+"], 3) {
            if !seq.is_empty() {
                out.push(seq.join("/"));
            }
            if seq.len() < 3 {
                let mut with_hash = seq.clone();
                with_hash.push("#");
                out.push(with_hash.join("/"));
            }
        }
        out
    }

    /// Topics of up to four levels, so every filter above that matches some
    /// topic also matches one of these.
    fn generated_topics() -> Vec<String> {
        sequences(&["a", "b", "$s", "", "z"], 4)
            .into_iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.join("/"))
            .collect()
    }

    #[test]
    fn matches_agrees_with_oracle() {
        let filters = generated_filters();
        let topics = generated_topics();
        for f in &filters {
            let fv: Vec<&str> = f.split('/').collect();
            for t in &topics {
                let tv: Vec<&str> = t.split('/').collect();
                let want =
                    is_valid_filter(f) && is_valid_topic(t) && oracle_matches(&fv, &tv, true);
                assert_eq!(matches(f, t), want, "{f:?} {t:?}");
            }
        }
    }

    /// The one pair that `overlap` reports and no valid topic confirms: both
    /// match only the empty string.
    fn is_empty_topic_pair(a: &str, b: &str) -> bool {
        matches!((a, b), ("+", "/#") | ("/#", "+"))
    }

    #[test]
    fn overlap_agrees_with_matches() {
        let filters = generated_filters();
        let topics = generated_topics();
        let matched: Vec<Vec<bool>> = filters
            .iter()
            .map(|f| topics.iter().map(|t| matches(f, t)).collect())
            .collect();
        let mut overlapping = 0usize;
        for (i, a) in filters.iter().enumerate() {
            for (j, b) in filters.iter().enumerate() {
                let want = matched[i].iter().zip(&matched[j]).any(|(x, y)| *x && *y);
                if is_empty_topic_pair(a, b) {
                    assert!(overlap(a, b) && !want, "{a:?} {b:?}");
                    continue;
                }
                assert_eq!(overlap(a, b), want, "{a:?} {b:?}");
                overlapping += usize::from(want);
            }
        }
        // Guard against a generator that produces nothing interesting.
        assert!(overlapping > 1_000, "{overlapping}");
    }

    #[test]
    fn overlap_is_symmetric_and_reflexive() {
        let filters = generated_filters();
        for a in &filters {
            assert_eq!(overlap(a, a), is_valid_filter(a), "{a:?}");
            for b in &filters {
                assert_eq!(overlap(a, b), overlap(b, a), "{a:?} {b:?}");
            }
        }
    }

    #[test]
    fn arbitrary_strings_do_not_panic() {
        let pieces = [
            "", "/", "+", "#", "$", "a", "\0", "\u{e9}", "//", "#/", "/#",
        ];
        for x in &pieces {
            for y in &pieces {
                for z in &pieces {
                    let s = format!("{x}{y}{z}");
                    let _ = is_valid_filter(&s);
                    let _ = is_valid_topic(&s);
                    for w in &pieces {
                        let _ = matches(&s, w);
                        let _ = matches(w, &s);
                        let _ = overlap(&s, w);
                    }
                }
            }
        }
    }
}
