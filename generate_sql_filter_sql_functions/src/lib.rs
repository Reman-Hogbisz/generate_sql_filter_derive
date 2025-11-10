use diesel::sql_types::Text;

diesel::define_sql_function!(fn length(x: Text) -> Integer);
