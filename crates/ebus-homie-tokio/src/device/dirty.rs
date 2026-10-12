//! Lock-free dirty marks: set from any thread, drained by the owner.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// One bit per slot. [`mark`](Self::mark) is safe from any thread;
/// [`drain`](Self::drain) is for the one owner.
#[derive(Debug)]
pub(crate) struct DirtySet {
    words: Box<[AtomicU32]>,
    bits: usize,
    cursor: AtomicUsize,
}

impl DirtySet {
    /// A set for indices `0..slots`, all clear.
    pub(crate) fn new(slots: usize) -> Self {
        let words: Vec<AtomicU32> = (0..slots.div_ceil(32)).map(|_| AtomicU32::new(0)).collect();
        DirtySet {
            words: words.into_boxed_slice(),
            bits: slots,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Sets bit `i`; true when it was clear, so the owner needs a wake. An
    /// index past the set's size is ignored and returns false.
    pub(crate) fn mark(&self, i: u32) -> bool {
        let i = i as usize;
        if i >= self.bits {
            return false;
        }
        let (Some(word), bit) = (self.words.get(i / 32), 1u32 << (i % 32)) else {
            return false;
        };
        word.fetch_or(bit, Ordering::AcqRel) & bit == 0
    }

    /// Clears and visits set bits, starting at the word where the last drain
    /// stopped, so low indices cannot starve high ones. `f` returning false
    /// stops after that index; the bits not yet visited stay set.
    pub(crate) fn drain(&self, mut f: impl FnMut(u32) -> bool) {
        let n = self.words.len();
        if n == 0 {
            return;
        }
        let start = self.cursor.load(Ordering::Relaxed) % n;
        for k in 0..n {
            let w = (start + k) % n;
            let Some(word) = self.words.get(w) else {
                continue;
            };
            // A load is cheaper than a swap; a bit set after it wakes the
            // owner for the next drain.
            if word.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let mut rest = word.swap(0, Ordering::AcqRel);
            while rest != 0 {
                let b = rest.trailing_zeros();
                rest &= rest - 1;
                // w < words.len() <= u32::MAX / 32 + 1 for any u32 index.
                let index = (w as u32).wrapping_mul(32).wrapping_add(b);
                if !f(index) {
                    if rest != 0 {
                        word.fetch_or(rest, Ordering::AcqRel);
                    }
                    self.cursor.store(w, Ordering::Relaxed);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(s: &DirtySet) -> Vec<u32> {
        let mut out = Vec::new();
        s.drain(|i| {
            out.push(i);
            true
        });
        out
    }

    #[test]
    fn mark_reports_clear_to_set_only() {
        let s = DirtySet::new(70);
        assert!(s.mark(3));
        assert!(!s.mark(3));
        assert!(s.mark(69));
        assert!(!s.mark(70), "past capacity");
        assert_eq!(all(&s), [3, 69]);
        assert!(s.mark(3), "clear again after a drain");
    }

    #[test]
    fn stopping_keeps_unvisited_bits_and_rotates() {
        let s = DirtySet::new(96);
        for i in [1, 2, 3, 40, 70] {
            s.mark(i);
        }
        let mut seen = vec![];
        s.drain(|i| {
            seen.push(i);
            i != 2
        });
        assert_eq!(seen, [1, 2]);
        // Restarts in the word where it stopped.
        assert_eq!(all(&s), [3, 40, 70]);
        // A stop in a later word makes the next drain start there.
        for i in [1, 40, 41] {
            s.mark(i);
        }
        let mut first = vec![];
        s.drain(|i| {
            first.push(i);
            i != 40
        });
        assert_eq!(first, [1, 40]);
        s.mark(0);
        assert_eq!(all(&s), [41, 0]);
    }

    #[test]
    fn empty_set() {
        let s = DirtySet::new(0);
        assert!(!s.mark(0));
        assert!(all(&s).is_empty());
    }
}
