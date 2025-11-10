use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::Path;

pub fn generate_string_filter_for_field(field_name: &str, sql_table: &Path) -> TokenStream {
    quote! {
        match (current_filter.operator, current_filter.values) {
            (generate_sql_filter_types::Operator::Is, [value]) => {
                query = query.filter(#sql_table::#field_name.eq(value));
            },
            (generate_sql_filter_types::Operator::IsNot, [value]) => {
                query = query.filter(!#sql_table::#field_name.ne(value));
            }
            (generate_sql_filter_types::Operator::Contains, [value]) => {
                query = query.filter(
                    #sql_table::#field_name.ilike(format!("%{value}%")),
                );
            }
            (generate_sql_filter_types::Operator::ContainsCaseSensitive, [value]) => {
                query = query.filter(
                    #sql_table::#field_name.like(format!("%{value}%")),
                );
            }
            (generate_sql_filter_types::Operator::NotContains, [value]) => {
                query = query.filter(
                    diesel::dsl::not(#sql_table::#field_name.ilike(format!("%{value}%"))),
                );
            }
            (generate_sql_filter_types::Operator::NotContainsCaseSensitive, [value]) => {
                query = query.filter(
                    diesel::dsl::not(#sql_table::#field_name.like(format!("%{value}%"))),
                );
            }
            (generate_sql_filter_types::Operator::StartsWith, [value])  =>  {
                query = query.filter(
                    #sql_table::#field_name.ilike(format!("{value}%")),
                );
            }
            (generate_sql_filter_types::Operator::StartsWithCaseSensitive, [value])  =>  {
                query = query.filter(
                    #sql_table::#field_name.like(format!("{value}%")),
                );
            }
            (generate_sql_filter_types::Operator::EndsWith, [value])   =>  {
                query = query.filter(
                    #sql_table::#field_name.ilike(format!("%{value}")),
                );
            }
            (generate_sql_filter_types::Operator::EndsWithCaseSensitive, [value])   =>  {
                query = query.filter(
                    #sql_table::#field_name.like(format!("%{value}")),
                );
            }
            (generate_sql_filter_types::Operator::Empty, _) => {
                query = query.filter(generate_sql_filter_sql_functions::length(#sql_table::#field_name).eq(0));
            }
            (generate_sql_filter_types::Operator::NotEmpty, _) => {
                query = query.filter(diesel::dsl::not(generate_sql_filter_sql_functions::length(#sql_table::#field_name).eq(0)));
            }
            (
                generate_sql_filter_types::Operator::Is |
                generate_sql_filter_types::Operator::IsNot |
                generate_sql_filter_types::Operator::Contains |
                generate_sql_filter_types::Operator::ContainsCaseSensitive |
                generate_sql_filter_types::Operator::NotContains |
                generate_sql_filter_types::Operator::NotContainsCaseSensitive  |
                generate_sql_filter_types::Operator::StartsWith |
                generate_sql_filter_types::Operator::StartsWithCaseSensitive |
                generate_sql_filter_types::Operator::EndsWith |
                generate_sql_filter_types::Operator::EndsWithCaseSensitive |
                generate_sql_filter_types::Operator::Empty |
                generate_sql_filter_types::Operator::NotEmpty
            , _) => {
                return Err(
                        SqlError::Other(
                            format!(
                                "Cannot filter on {}::{} op {} because it does not have the correct number of values ({} expected 1).",
                                stringify!(#sql_table),
                                #field_name,
                                current_filter.operator,
                                current_filter.values.len()
                )));
            }
            (_, _) => {
                return Err(
                        SqlError::Other(
                            format!(
                                "Cannot filter on {}::{} op {} because the operator provided is invalid.",
                                stringify!(#sql_table),
                                #field_name,
                                current_filter.operator
                )));
            }
        }
    }
}

pub fn generate_optional_string_filter_for_field(
    field_name: &str,
    sql_table: &Path,
) -> TokenStream {
    quote! {
        if current_filter.is_some {
            match (current_filter.operator, current_filter.values) {
                (generate_sql_filter_types::Operator::Is, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .eq(value)
                    ));
                },
                (generate_sql_filter_types::Operator::IsNot, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .ne(value)
                    ));
                }
                (generate_sql_filter_types::Operator::Contains, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .ilike(format!("%{value}%"))
                    ));
                }
                (generate_sql_filter_types::Operator::ContainsCaseSensitive, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .like(format!("%{value}%"))
                    ));
                }
                (generate_sql_filter_types::Operator::NotContains, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                diesel::dsl::not(#sql_table::#field_name.assume_not_null()
                                    .ilike(format!("%{value}%")))
                    ));
                }
                (generate_sql_filter_types::Operator::NotContainsCaseSensitive, [value]) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                diesel::dsl::not(#sql_table::#field_name.assume_not_null()
                                    .like(format!("%{value}%")))
                    ));
                }
                (generate_sql_filter_types::Operator::StartsWith, [value])  =>  {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .ilike(format!("{value}%"))
                    ));
                }
                (generate_sql_filter_types::Operator::StartsWithCaseSensitive, [value])  =>  {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .like(format!("{value}%"))
                    ));
                }
                (generate_sql_filter_types::Operator::EndsWith, [value])   =>  {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .ilike(format!("%{value}"))
                    ));
                }
                (generate_sql_filter_types::Operator::EndsWithCaseSensitive, [value])   =>  {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                #sql_table::#field_name.assume_not_null()
                                    .like(format!("%{value}"))
                    ));
                }
                (generate_sql_filter_types::Operator::Empty, _) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                generate_sql_filter_sql_functions::length(#sql_table::#field_name.assume_not_null()).eq(0)
                    ));
                }
                (generate_sql_filter_types::Operator::NotEmpty, _) => {
                    query = query.filter(
                        #sql_table::.#field_name.is_not_null()
                            .and(
                                diesel::dsl::not(generate_sql_filter_sql_functions::length(#sql_table::#field_name.assume_not_null()).eq(0))
                    ));
                }
                (
                    generate_sql_filter_types::Operator::Is |
                    generate_sql_filter_types::Operator::IsNot |
                    generate_sql_filter_types::Operator::Contains |
                    generate_sql_filter_types::Operator::ContainsCaseSensitive |
                    generate_sql_filter_types::Operator::NotContains |
                    generate_sql_filter_types::Operator::NotContainsCaseSensitive  |
                    generate_sql_filter_types::Operator::StartsWith |
                    generate_sql_filter_types::Operator::StartsWithCaseSensitive |
                    generate_sql_filter_types::Operator::EndsWith |
                    generate_sql_filter_types::Operator::EndsWithCaseSensitive |
                    generate_sql_filter_types::Operator::Empty |
                    generate_sql_filter_types::Operator::NotEmpty
                , _) => {
                    return Err(
                            SqlError::Other(
                                format!(
                                    "Cannot filter on {}::{} op {} because it does not have the correct number of values ({} expected 1).",
                                    stringify!(#sql_table),
                                    #field_name,
                                    current_filter.operator,
                                    current_filter.values.len()
                    )));
                }
                (_, _) => {
                    return Err(
                            SqlError::Other(
                                format!(
                                    "Cannot filter on {}::{} op {} because the operator provided is invalid.",
                                    stringify!(#sql_table),
                                    #field_name,
                                    current_filter.operator
                    )));
                }
            }
        } else {
            query = query.filter(
                #sql_table::#field_name.is_null()
            );
        }
    }
}
