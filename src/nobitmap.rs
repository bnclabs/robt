//! Module `nobitmap` define a dummy bitmap index.

use mkit::traits::Bloom;

pub struct NoBitmap;

impl Bloom for NoBitmap {
    #[inline]
    fn create() -> Self {
        NoBitmap
    }

    #[inline]
    fn len(&self) -> mkit::Result<usize> {
        Ok(0)
    }

    #[inline]
    fn add_key<Q: ?Sized>(&mut self, _element: &Q) {
        // Do nothing.
    }

    #[inline]
    fn add_digest32(&mut self, _digest: u32) {
        // Do nothing.
    }

    #[inline]
    fn contains<Q: ?Sized>(&self, _element: &Q) -> bool {
        true // false positives are okay.
    }

    #[inline]
    fn to_vec(&self) -> Vec<u8> {
        vec![]
    }

    #[inline]
    fn from_vec(_buf: &[u8]) -> mkit::Result<NoBitmap> {
        Ok(NoBitmap)
    }

    #[inline]
    fn or(&self, _other: &NoBitmap) -> mkit::Result<NoBitmap> {
        Ok(NoBitmap)
    }
}
