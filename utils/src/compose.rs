//credit: https://github.com/Kushagra-0801/fn-compose/blob/master/src/lib.rs
#[macro_export]
macro_rules! compose {
    ( $arg: expr ) => { $arg };
    ( $arg: expr => $f: ident(_) ) => { $f($arg) };
    ( $arg: expr => $f: ident( $( $largs: expr, )* _ $( ,$rargs: expr )*) ) => {
        $f($($largs,)* $arg $(,$rargs)*)
    };
    ( $arg: expr => $f: ident(_) => $( $tokens: tt )* ) => {{
        compose!($f($arg) => $($tokens)*)
    }};
    ( $arg: expr => $f: ident( $( $largs: expr, )* _ $( ,$rargs: expr )*) => $( $tokens: tt )* ) => {{
        compose!($f($($largs,)* $arg $(,$rargs)*) => $($tokens)*)
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn no_fn() {
        assert_eq!(compose!(4), 4);
        let ret_4 = || {
            // ...
            // Doing something expensive..
            4
        };
        assert_eq!(compose!(ret_4()), 4);
    }

    #[test]
    fn one_simple_fn() {
        let add_one = |x| x + 1;
        assert_eq!(compose!(4 => add_one(_)), 5);
    }

    #[test]
    fn one_arg_fn() {
        let add = |x, y| x + y;
        assert_eq!(compose!(4 => add(5, _)), 9);
    }

    #[test]
    fn two_simple_fn() {
        let add_one = |x| x + 1;
        let double = |x| 2 * x;
        assert_eq!(compose!(4 => double(_) => add_one(_)), 9);
    }

    #[test]
    fn two_arg_fn() {
        let mul = |a, b, c, d, e| a * b * c * d * e;
        let add = |a, b, c| a + b + c;
        assert_eq!(compose!(4 => add(3, _, 9) => mul(1, 4, 2, _, 10)), 1280);
    }

    #[test]
    fn add_vec_fn() {
        let sum = |v: Vec<i32>| v.iter().sum();
        let mul = |a: i32, b: i32| a * b;

        assert_eq!(compose!(sum(vec![1, 2, 3]) => mul(_, 10)), 60);
    }
}
