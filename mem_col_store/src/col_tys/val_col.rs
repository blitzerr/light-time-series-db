use roaring::RoaringBitmap;

#[derive(Debug)]
pub struct ValCol {
    pub rows: Vec<f64>,
}

impl ValCol {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn append(&mut self, mut v: Vec<f64>) {
        self.rows.append(&mut v);
    }

    pub fn with_vals(vals: Vec<f64>) -> Self {
        Self { rows: vals }
    }

    pub fn get(&self, bm: &RoaringBitmap) -> Vec<f64> {
        // Clone below is okay as it only copies the heap pointer of the interned string.
        bm.into_iter()
            .map(|i| self.rows[i as usize].clone())
            .collect()
    }
}
