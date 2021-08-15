//! This takes a query and runs it. A query about three things: window of analysis, buckets of time,
//! [metrics, [filters, aggregations]]. This could be done over multiple metrics and each of them
//! can have their unique filters and aggregations defined.
use quickersort;

type Val = f64;

macro_rules! sum {
    ($i: ident) => (Some($i.iter().sum::<f64>()));
    ($($args:expr),*) => {{
        let result = 0.0;
        $(
            let result = result + $args;
        )*
        Some(result)
    }};


}

macro_rules! max {
    ($i: ident) => {
        {
            let x = $i.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
            if x == f64::NEG_INFINITY {
                None
            } else {
                Some(x)
            }
        }
    };
    ($($args:expr),*) => {{
        let result = f64::NEG_INFINITY;
        $(
            let result = result.max($args);
        )*
        if result == f64::NEG_INFINITY {
            None
        } else {
            Some(result)
        }
    }};
}

macro_rules! min {
    ($i: ident) => {
        {
            let x = $i.iter().fold(f64::INFINITY, |a, b| a.min(*b));
            if x == f64::INFINITY {
                None
            } else {
                Some(x)
            }
        }
    };
    ($($args:expr),*) => {{
        let result = f64::INFINITY;
        $(
            let result = result.min($args);
        )*
        if result == f64::INFINITY {
            None
        } else {
            Some(result)
        }
    }};
}

macro_rules! avg {
    ($i: ident) => {{
        let ct = $i.len() as f64;
        if ct > 0.0 {
            sum!($i).map(|s| s / ct )
        } else {
            None
        }
    }};

    ($($args:expr),*) => {{
        let ct = 0.0;
        let s = 0.0;
        $(
            let ct = ct + 1.0;
            let s = s + $args;
        )*
        Some(s / ct)
    }};
}

pub(crate) fn median_vec(mut v: &mut Vec<Val>) -> Option<Val> {
    if v.is_empty() {
        None
    } else {
        quickersort::sort_floats(&mut v);
        let mid = v.len() / 2;
        if v.len() % 2 == 0 {
            avg!(v[mid - 1], v[mid])
        } else {
            Some(v[mid])
        }
    }
}

macro_rules! median {
    ($i: ident) => {{
       median_vec(&mut $i)
    }};

    ($($args:expr),*) => {{
        let mut v = vec![];
        $(
            v.push($args);
        )*
        median_vec(&mut v)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t_sum() {
        let x = sum!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(27.0, x.unwrap());

        let mut x = Vec::new();
        x.push(2.0);
        x.push(3.0);
        x.push(4.0);
        x.push(5.0);
        x.push(6.0);
        x.push(7.0);

        let x: f64 = sum!(x).unwrap();
        assert_eq!(27.0, x);

        let v = vec![];
        let s = sum!(v);
        println!("{:?}", &s);
    }

    #[test]
    fn t_max() {
        let x = max!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(7.0, x.unwrap());

        let mut x = Vec::new();
        x.push(2.0);
        x.push(3.0);
        x.push(4.0);
        x.push(5.0);
        x.push(6.0);
        x.push(7.0);

        let x = max!(x);
        assert_eq!(7.0, x.unwrap());
    }

    #[test]
    fn t_min() {
        let x = min!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(2.0, x.unwrap());

        let mut x = Vec::new();
        x.push(2.0);
        x.push(3.0);
        x.push(4.0);
        x.push(5.0);
        x.push(6.0);
        x.push(7.0);

        let x = min!(x);
        assert_eq!(2.0, x.unwrap());
    }

    #[test]
    fn t_avg() {
        let x = avg!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(4.5, x.unwrap());

        let mut x = Vec::new();
        x.push(2.0);
        x.push(3.0);
        x.push(4.0);
        x.push(5.0);
        x.push(6.0);
        x.push(7.0);

        let x: f64 = avg!(x).unwrap();
        assert_eq!(4.5, x);
    }

    #[test]
    fn t_median() {
        let x = median!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(4.5, x.unwrap());

        let mut x = Vec::new();
        x.push(2.0);
        x.push(3.0);
        x.push(4.0);
        x.push(5.0);
        x.push(6.0);
        x.push(7.0);

        let x: f64 = median!(x).unwrap();
        assert_eq!(4.5, x);

        let x = median!(2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 100.0);
        assert_eq!(5.0, x.unwrap());
    }

    // min(
    //   max(
    //      avg(x1, x2, x3), avg(y1, y2, y3)
    //   ),
    //   max(
    //      avg(x1, x2, x3), avg(y1, y2, y3)
    //   )
    // )
    #[test]
    fn t_compose() {
        let res = sum!(
            max!(avg!(1.0, 2.0, 3.0).unwrap(), avg!(4.0, 5.0, 6.0).unwrap()).unwrap(), // max (2, 5) -> 5
            min!(
                median!(1.0, 2.0, 3.0).unwrap(),
                median!(4.0, 5.0, 6.0, 7.0).unwrap()
            )
            .unwrap()  // min (2, 5.5) -> 2
        );
        assert_eq!(7.0, res.unwrap());
    }
}
