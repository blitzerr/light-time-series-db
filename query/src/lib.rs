#[macro_use]
mod aggs;
pub mod filters;
mod planner;
mod qry;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
