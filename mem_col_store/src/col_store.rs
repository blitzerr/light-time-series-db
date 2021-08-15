use crate::col_tys::{str_col::StrCol, tag_col::TagCol, ts_col::TsCol, val_col::ValCol};

#[derive(Debug)]
struct ColStore {
    timestamps: TsCol,
    metrics: StrCol,
    values: ValCol,
    tags: TagCol,
}

impl ColStore {
    // pub fn put_stream(
    //     &mut self,
    //     data_points: Vec<(u64, &str, f64, (&str, Vec<&str>))>,
    // ) -> color_eyre::Result<()> {
    //     let mut ts = vec![];
    //     let mut metric = vec![];
    //     let mut vals = vec![];
    //     let mut tags = vec![];

    //     data_points.into_iter().for_each(|x| {
    //         ts.push(x.0);
    //         metric.push(x.1);
    //         vals.push(x.2);
    //         tags.push(x.3);
    //     });

    //     self.timestamps.append(&mut ts);
    //     self.metrics.append(metric);
    //     self.values.append(vals);
    //     self.tags.append(tags);

    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {
    use internment::ArcIntern;

    use crate::search_based;

    #[test]
    fn t_intern() {
        let hello = ArcIntern::new("hello");
        let hello2 = ArcIntern::new("hello");
        let world = ArcIntern::new("world");

        println!("{}, {}", hello, hello2);
        assert_eq!(hello, hello2);
        assert_ne!(hello, world);
        assert_eq!(ArcIntern::<&str>::num_objects_interned(), 2);
        assert_eq!(hello.refcount(), 2);
        assert_eq!(world.refcount(), 1);
    }
}
