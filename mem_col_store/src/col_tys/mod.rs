use internment::ArcIntern;

pub mod str_col;
pub mod ts_col;
pub mod val_col;
pub mod tag_col;

pub type Str = ArcIntern<String>;

trait ColTy {
}
