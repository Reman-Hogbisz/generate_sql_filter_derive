use proc_macro::{self, TokenStream};
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, DeriveInput, FieldsNamed};

fn to_camel_case<S: ToString>(s_raw: S) -> String {
    let mut s = s_raw.to_string();
    if let Some(r) = s.get_mut(0..1) {
        r.make_ascii_uppercase();
    }

    let mut chars = s.chars().peekable();
    let mut output = String::new();
    while let Some(c) = chars.next() {
        if c == '_' {
            if let Some(r) = chars.peek_mut() {
                r.make_ascii_uppercase();
            }
        } else {
            output.push(c);
        }
    }

    output
}

#[proc_macro_derive(CreateFilter, attributes(filter_name, sql_path))]
pub fn create_filter(input: TokenStream) -> TokenStream {
    let DeriveInput {
        ident, data, attrs, ..
    } = parse_macro_input!(input);

    let filter_name_attribute = attrs
        .iter()
        .find(|attr| attr.path.is_ident("filter_name"))
        .expect("derive(CreateFilter) requires a #[filter_name(...)] attribute with ");

    let struct_name = if let syn::Meta::List(list) = filter_name_attribute
        .parse_meta()
        .expect("Failed to parse metadata of filter_name attribute")
    {
        list.nested.iter().find_map(|nested| {
            if let syn::NestedMeta::Lit(lit_val) = nested {
                return Some(syn::Ident::new(
                    &lit_val.to_token_stream().to_string().replace("\"", ""),
                    ident.span(),
                ));
            }
            None
        })
    } else {
        None
    };

    if struct_name.is_none() {
        panic!("derive(CreateFilter) requires a filter_name attribute");
    }

    let struct_name = struct_name.unwrap();

    let sql_path_attribute = attrs
        .iter()
        .find(|attr| attr.path.is_ident("sql_path"))
        .expect("derive(CreateFilter) requires a #[sql_path(...)] attribute with the path to the schema from diesel");

    let sql_table = if let syn::Meta::List(list) = sql_path_attribute
        .parse_meta()
        .expect("Failed to parse metadata of sql_path attribute")
    {
        list.nested.iter().find_map(|nested| {
            if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = nested {
                Some(path.clone())
            } else {
                None
            }
        })
    } else {
        None
    };

    if sql_table.is_none() {
        panic!("derive(CreateFilter) requires a sql_path attribute");
    }

    // let sql_table = sql_table.unwrap();

    let struct_token = match data {
        syn::Data::Struct(s) => s,
        _ => panic!("derive(CreateFilter) only supports structs"),
    };

    let fields = match struct_token.fields {
        syn::Fields::Named(FieldsNamed { named, .. }) => named,
        _ => panic!("derive(CreateFilter) only supports named fields"),
    };

    let (idents, types): (Vec<_>, Vec<_>) = fields
        .iter()
        .filter_map(|f| match f.ident {
            Some(ref i) => Some((i, &f.ty)),
            None => None,
        })
        .unzip();

    let enum_name = Ident::new(
        &format!("{}SortBy", ident.to_string()),
        Span::call_site(),
    );

    let mut filtered_field_declarations = TokenStream2::default();

    let mut field_sort_by_enum_declarations = TokenStream2::default();

    let mut field_sort_declarations = TokenStream2::default();

    let mut query_builder_declarations = TokenStream2::default();

    const TYPES_THAT_HAVE_ORDERING: &[&'static str] = &[
        "f64",
        "i64",
        "u64",
        "f32",
        "i32",
        "u32",
        "i16",
        "u16",
        "i8",
        "u8",
        "usize",
        "isize",
    ];

    const TYPES_WITH_PARTIAL_EQ: &[&'static str] = &[
        "f64",
        "i64",
        "u64",
        "f32",
        "i32",
        "u32",
        "i16",
        "u16",
        "i8",
        "u8",
        "usize",
        "isize",
        "String",
        "bool",
        "char",
        "Uuid",
    ];

    idents
        .into_iter()
        .zip(types.into_iter())
        .for_each(|(field, ftype)| {
            match ftype {
                syn::Type::Array(t) => {
                    t.elem
                        .to_token_stream()
                        .into_iter()
                        .for_each(|token| match token {
                            proc_macro2::TokenTree::Ident(ident) => {
                                let ident_string = ident.to_string();
                                let ident_str = ident_string.as_str();
                                if TYPES_WITH_PARTIAL_EQ.contains(&ident_str) {
                                    let field_contains = Ident::new(
                                        &format!("{}_contains", field),
                                        Span::call_site(),
                                    );
                                    let field_contains_any = Ident::new(
                                        &format!("{}_contains_any", field),
                                        Span::call_site(),
                                    );
                                    let inner_type = t.elem.to_token_stream();
                                    filtered_field_declarations.extend::<TokenStream2>(quote! {
                                        pub #field_contains : Option<#inner_type>,
                                        pub #field_contains_any : Option<Vec<#inner_type>>,
                                    });

                                    query_builder_declarations.extend::<TokenStream2>(quote! {
                                        if let Some(contains) = self.#field_contains {
                                            query_builder = query_builder.filter(#sql_table::#field.eq_any(contains));
                                        }

                                        if let Some(contains_any) = self.#field_contains_any {
                                            query = query.filter(#field.overlaps_with(contains_any));
                                        }
                                    });
                                }
                            }
                            _ => (),
                        });
                }
                syn::Type::Path(t) => {
                    // If the type is a path, we need to check if it inherits comparable
                    // traits. If it does, we can filter it.
                    if let Some(ident) = t.path.get_ident() {
                        let ident_string = ident.to_string();
                        let ident_str = ident_string.as_str();

                        match ident_str {
                            "String" => {
                                let field_contains =
                                    Ident::new(&format!("{}_contains", field), Span::call_site());
                                let field_not_contains =
                                    Ident::new(&format!("{}_not_contains", field), Span::call_site());
                                let field_starts_with = Ident::new(
                                    &format!("{}_starts_with", field),
                                    Span::call_site(),
                                );
                                let field_not_starts_with = Ident::new(
                                    &format!("{}_not_starts_with", field),
                                    Span::call_site(),
                                );
                                let field_ends_with =
                                    Ident::new(&format!("{}_ends_with", field), Span::call_site());
                                let field_not_ends_with = Ident::new(
                                    &format!("{}_not_ends_with", field),
                                    Span::call_site(),
                                );
                                let field_contains_insensitive =
                                    Ident::new(&format!("{}_contains_insensitive", field), Span::call_site());
                                let field_not_contains_insensitive =
                                    Ident::new(&format!("{}_not_contains_insensitive", field), Span::call_site());
                                let field_starts_with_insensitive = Ident::new(
                                    &format!("{}_starts_with_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_not_starts_with_insensitive = Ident::new(
                                    &format!("{}_not_starts_with_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_ends_with_insensitive =
                                    Ident::new(&format!("{}_ends_with_insensitive", field), Span::call_site());
                                let field_not_ends_with_insensitive = Ident::new(
                                    &format!("{}_not_ends_with_insensitive", field),
                                    Span::call_site(),
                                );
                                filtered_field_declarations.extend::<TokenStream2>(quote! {
                                    pub #field_contains : Option<String>,
                                    pub #field_not_contains: Option<String>,
                                    pub #field_starts_with : Option<String>,
                                    pub #field_not_starts_with: Option<String>,
                                    pub #field_ends_with : Option<String>,
                                    pub #field_not_ends_with: Option<String>,
                                    pub #field_contains_insensitive : Option<String>,
                                    pub #field_not_contains_insensitive : Option<String>,
                                    pub #field_not_starts_with_insensitive : Option<String>,
                                    pub #field_starts_with_insensitive : Option<String>,
                                    pub #field_not_ends_with_insensitive : Option<String>,
                                    pub #field_ends_with_insensitive : Option<String>,
                                });

                                query_builder_declarations.extend::<TokenStream2>(quote! {
                                    if let Some(contains) = self.#field_contains.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("%{}%", contains)));
                                    }

                                    if let Some(not_contains) = self.#field_not_contains.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_like(format!("%{}%", not_contains)));
                                    }

                                    if let Some(starts_with) = self.#field_starts_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("{}%", starts_with)));
                                    }

                                    if let Some(not_starts_with) = self.#field_not_starts_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_like(format!("{}%", not_starts_with)));
                                    }

                                    if let Some(ends_with) = self.#field_ends_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("%{}", ends_with)));
                                    }

                                    if let Some(not_ends_with) = self.#field_not_ends_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_like(format!("%{}", not_ends_with)));
                                    }

                                    if let Some(contains_insensitive) = self.#field_contains_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.ilike(format!("%{}%", contains_insensitive)));
                                    }

                                    if let Some(not_contains_insensitive) = self.#field_not_contains_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_ilike(format!("%{}%", not_contains_insensitive)));
                                    }

                                    if let Some(starts_with_insensitive) = self.#field_starts_with_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.ilike(format!("{}%", starts_with_insensitive)));
                                    }

                                    if let Some(not_starts_with_insensitive) = self.#field_not_starts_with_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_ilike(format!("{}%", not_starts_with_insensitive)));
                                    }

                                    if let Some(ends_with_insensitive) = self.#field_ends_with_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.ilike(format!("%{}", ends_with_insensitive)));
                                    }

                                    if let Some(not_ends_with_insensitive) = self.#field_not_ends_with_insensitive.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.not_ilike(format!("%{}", not_ends_with_insensitive)));
                                    }
                                });
                            }
                            _ => (),
                        }

                        if TYPES_THAT_HAVE_ORDERING.contains(&ident_str) || t.to_token_stream().into_iter().any(|token| match token {
                            proc_macro2::TokenTree::Ident(ident) => {
                                TYPES_THAT_HAVE_ORDERING.contains(&ident.to_string().as_str())
                            }
                            _ => false,
                        }){
                            let field_gt = Ident::new(&format!("{}_gt", field), Span::call_site());
                            let field_gte =
                                Ident::new(&format!("{}_gte", field), Span::call_site());
                            let field_lt = Ident::new(&format!("{}_lt", field), Span::call_site());
                            let field_lte =
                                Ident::new(&format!("{}_lte", field), Span::call_site());
                            filtered_field_declarations.extend::<TokenStream2>(quote! {
                                pub #field_gt : Option<#ftype>,
                                pub #field_gte : Option<#ftype>,
                                pub #field_lt : Option<#ftype>,
                                pub #field_lte : Option<#ftype>,
                            });

                            query_builder_declarations.extend::<TokenStream2>(quote! {
                                if let Some(gt) = self.#field_gt {
                                    query_builder = query_builder.filter(#sql_table::#field.gt(gt));
                                }

                                if let Some(gte) = self.#field_gte {
                                    query_builder = query_builder.filter(#sql_table::#field.ge(gte));
                                }

                                if let Some(lt) = self.#field_lt {
                                    query_builder = query_builder.filter(#sql_table::#field.lt(lt));
                                }

                                if let Some(lte) = self.#field_lte {
                                    query_builder = query_builder.filter(#sql_table::#field.le(lte));
                                }
                            });
                        }
                    } else {
                        if ftype
                            .to_token_stream()
                            .into_iter()
                            .any(|token| match token {
                                proc_macro2::TokenTree::Ident(ident) => {
                                    ident.to_string().as_str() == "Option"
                                }
                                _ => false,
                            })
                        {
                            let field_is_some =
                                Ident::new(&format!("{}_is_some", field), Span::call_site());
                            let field_is_some_in =
                                Ident::new(&format!("{}_is_some_in", field), Span::call_site());
                            let field_is_not_some =
                                Ident::new(&format!("{}_is_not_some", field), Span::call_site());
                            let field_is_not_some_in =
                                Ident::new(&format!("{}_is_not_some_in", field), Span::call_site());
                            let field_is_none =
                                Ident::new(&format!("{}_is_none", field), Span::call_site());

                            filtered_field_declarations.extend::<TokenStream2>(quote! {
                                pub #field_is_some : #ftype,
                                pub #field_is_some_in : Option<Vec<#ftype>>,
                                pub #field_is_not_some : #ftype,
                                pub #field_is_not_some_in : Option<Vec<#ftype>>,
                                pub #field_is_none : Option<bool>,
                            });

                            query_builder_declarations.extend::<TokenStream2>(quote! {
                                if let Some(is_some) = &self.#field_is_some {
                                    query_builder = query_builder.filter(#sql_table::#field.eq(is_some));
                                }

                                if let Some(is_some_in) = &self.#field_is_some_in {
                                    query_builder = query_builder.filter(#sql_table::#field.eq_any(is_some_in));
                                }

                                if let Some(is_not_some) = &self.#field_is_not_some {
                                    if self.#field_is_none.is_none() {
                                        query_builder = query_builder.filter(#sql_table::#field.ne(is_not_some).or(#sql_table::#field.is_null()));
                                    } else {
                                        query_builder = query_builder.filter(#sql_table::#field.ne(is_not_some));
                                    }
                                }

                                if let Some(is_not_some_in) = &self.#field_is_not_some_in {
                                    if self.#field_is_none.is_none() {
                                        query_builder = query_builder.filter(#sql_table::#field.ne_all(is_not_some_in).or(#sql_table::#field.is_null()));
                                    } else {
                                        query_builder = query_builder.filter(#sql_table::#field.ne_all(is_not_some_in));
                                    }
                                }

                                if let Some(is_none) = self.#field_is_none {
                                    if is_none {
                                        query_builder = query_builder.filter(#sql_table::#field.is_null());
                                    } else {
                                        query_builder = query_builder.filter(#sql_table::#field.is_not_null());
                                    }
                                }
                            });

                            let sort_by_field_name = to_camel_case(field.to_string());
                            let sort_by_field = Ident::new(
                                &sort_by_field_name,
                                Span::call_site(),
                            );
                            field_sort_by_enum_declarations.extend::<TokenStream2>(quote! {
                                #sort_by_field,
                            });

                            field_sort_declarations.extend::<TokenStream2>(quote! {
                                #enum_name::#sort_by_field => match sort_order {
                                    FilterSortOrder::Asc => {
                                        if cfg!(debug_assertions) {
                                            debug!("Sorting by {} ({}) in ascending order", stringify!(#sort_by_field), stringify!(#sql_table::#field));
                                        }
                                        query_builder = query_builder.order(#sql_table::#field.asc());
                                    },
                                    FilterSortOrder::Desc => {
                                        if cfg!(debug_assertions) {
                                            debug!("Sorting by {} ({}) in descending order", stringify!(#sort_by_field), stringify!(#sql_table::#field));
                                        }
                                        query_builder = query_builder.order(#sql_table::#field.desc());
                                    },
                                },
                            });
                        
                            return;
                        }
                        else if ftype
                        .to_token_stream()
                        .into_iter()
                        .any(|token| match token {
                            proc_macro2::TokenTree::Ident(ident) => {
                                // NaiveDate/Time, Date/Time<_>
                                ident.to_string().contains("Date")
                            }
                            _ => false,
                        }) {
                            let field_gt = Ident::new(&format!("{}_gt", field), Span::call_site());
                            let field_gte =
                                Ident::new(&format!("{}_gte", field), Span::call_site());
                            let field_lt = Ident::new(&format!("{}_lt", field), Span::call_site());
                            let field_lte =
                                Ident::new(&format!("{}_lte", field), Span::call_site());
                            filtered_field_declarations.extend::<TokenStream2>(quote! {
                                pub #field_gt : Option<#ftype>,
                                pub #field_gte : Option<#ftype>,
                                pub #field_lt : Option<#ftype>,
                                pub #field_lte : Option<#ftype>,
                            });

                            query_builder_declarations.extend::<TokenStream2>(quote! {

                                if let Some(gt) = self.#field_gt {
                                    query_builder = query_builder.filter(#sql_table::#field.gt(gt));
                                }

                                if let Some(gte) = self.#field_gte {
                                    query_builder = query_builder.filter(#sql_table::#field.ge(gte));
                                }

                                if let Some(lt) = self.#field_lt {
                                    query_builder = query_builder.filter(#sql_table::#field.lt(lt));
                                }

                                if let Some(lte) = self.#field_lte {
                                    query_builder = query_builder.filter(#sql_table::#field.le(lte));
                                }
                            });

                        } else {
                            return;
                        }
                    }
                }
                _ => {
                    return;
                }
            }
            let field_in =
                Ident::new(&format!("{}_in", field), Span::call_site());
            let field_not_in =
                Ident::new(&format!("{}_not_in", field), Span::call_site());
            let field_not = Ident::new(&format!("{}_not", field), Span::call_site());
            filtered_field_declarations.extend::<TokenStream2>(quote! { 
                pub #field : Option<#ftype>, 
                pub #field_not : Option<#ftype>,
                pub #field_in : Option<Vec<#ftype>>,
                pub #field_not_in : Option<Vec<#ftype>>,
            });
            query_builder_declarations.extend::<TokenStream2>(quote! {
                if let Some(value) = &self.#field {
                    query_builder = query_builder.filter(#sql_table::#field.eq(value));
                }

                if let Some(value) = &self.#field_not {
                    query_builder = query_builder.filter(#sql_table::#field.ne(value));
                }

                if let Some(val_in) = self.#field_in.as_ref() {
                    query_builder = query_builder.filter(#sql_table::#field.eq_any(val_in));
                }

                if let Some(val_not_in) = self.#field_not_in.as_ref() {
                    query_builder = query_builder.filter(#sql_table::#field.ne_all(val_not_in));
                }
            });

            let sort_by_field_name = to_camel_case(field.to_string());
            let sort_by_field = Ident::new(
                &sort_by_field_name,
                Span::call_site(),
            );
            field_sort_by_enum_declarations.extend::<TokenStream2>(quote! {
                #sort_by_field,
            });

            field_sort_declarations.extend::<TokenStream2>(quote! {
                #enum_name::#sort_by_field => match sort_order {
                    FilterSortOrder::Asc => {
                        if cfg!(debug_assertions) {
                            debug!("Sorting by {} ({}) in ascending order", stringify!(#sort_by_field), stringify!(#sql_table::#field));
                        }
                        query_builder = query_builder.order(#sql_table::#field.asc());
                    },
                    FilterSortOrder::Desc => {
                        if cfg!(debug_assertions) {
                            debug!("Sorting by {} ({}) in descending order", stringify!(#sort_by_field), stringify!(#sql_table::#field));
                        }
                        query_builder = query_builder.order(#sql_table::#field.desc());
                    },
                },
            });
        });

    let sql_filter_function_name = Ident::new(
        &format!("filter_{}", ident.to_string().to_lowercase()),
        Span::call_site(),
    );

    let sql_filter_with_count_function_name = Ident::new(
        &format!("filter_with_count_{}", ident.to_string().to_lowercase()),
        Span::call_site(),
    );

    let sql_count_function_name = Ident::new(
        &format!("count_{}", ident.to_string().to_lowercase()),
        Span::call_site(),
    );

    let output = quote! {

        use crate::util::*;
        use crate::db_connection::*;
        use diesel::prelude::*;

        #[derive(TS, Clone, Copy, Debug, Deserialize, PartialEq)]
        #[ts(export)]
        pub enum #enum_name {
            #field_sort_by_enum_declarations
        }

        #[derive(TS, Default, Clone, Debug, Deserialize, PartialEq)]
        #[ts(export)]
        pub struct #struct_name {
            pub limit: Option<i32>,
            pub page: Option<i32>,
            pub sort_by: Option<#enum_name>,
            pub sort_order: Option<FilterSortOrder>,
            #filtered_field_declarations
        } impl #struct_name {

            pub fn #sql_filter_function_name(
                &self,
                pool: &PgPool,
            ) -> Result<Vec<#ident>, SqlError> {

                let connection = match pool.get() {
                    Ok(connection) => connection,
                    Err(e) => {
                        error!("Failed to get pooled connection with error '{}'", e);
                        return Err(SqlError::ConnectionError);
                    }
                };

                let mut limit = self.limit.unwrap_or(100) as i64;
                if limit < 0 {
                    limit = 100;
                }

                let mut page = self.page.unwrap_or(0) as i64;
                if page < 0 {
                    page = 0;
                }

                let mut query_builder = #sql_table::table
                    .offset(page * limit)
                    .limit(limit)
                    .into_boxed();

                #query_builder_declarations

                match (self.sort_by, self.sort_order) {
                    (Some(sort_by), Some(sort_order)) => {
                        if cfg!(debug_assertions) {
                            debug!("Query Filter found Some(sort_by) = {:?} and Some(sort_order) = {:?}", sort_by, sort_order);
                        }
                        match sort_by {
                            #field_sort_declarations
                        }
                    },
                    _ => {}
                }

                if cfg!(debug_assertions) {
                    let debug = diesel::debug_query::<diesel::pg::Pg, _>(&query_builder);
                    debug!("Filter Query: {}", debug);
                }

                match query_builder.load::<#ident>(&connection) {
                    Ok(vals) => Ok(vals),
                    Err(e) => {
                        error!("Failed to get {} with error '{}'", stringify!(#ident), e);
                        return Err(SqlError::DieselError(e));
                    }
                }
            }

            pub fn #sql_filter_with_count_function_name(
                &self,
                pool: &PgPool,
            ) -> Result<(Vec<#ident>, i64), SqlError> {

                let connection = match pool.get() {
                    Ok(connection) => connection,
                    Err(e) => {
                        error!("Failed to get pooled connection with error '{}'", e);
                        return Err(SqlError::ConnectionError);
                    }
                };

                let mut limit = self.limit.unwrap_or(100) as i64;
                if limit < 0 {
                    limit = 100;
                }

                let mut page = self.page.unwrap_or(0) as i64;
                if page < 0 {
                    page = 0;
                }

                let mut query_builder = #sql_table::table
                    .into_boxed();

                #query_builder_declarations

                let count = match query_builder.count().get_result::<i64>(&connection) {
                    Ok(count) => count,
                    Err(e) => {
                        error!("Failed to get count of {} with error '{}'", stringify!(#ident), e);
                        return Err(SqlError::DieselError(e));
                    }
                };
                
                let mut query_builder = #sql_table::table
                    .offset(page * limit)
                    .limit(limit)
                    .into_boxed();

                #query_builder_declarations

                match (self.sort_by, self.sort_order) {
                    (Some(sort_by), Some(sort_order)) => {
                        if cfg!(debug_assertions) {
                            debug!("Query Filter found Some(sort_by) = {:?} and Some(sort_order) = {:?}", sort_by, sort_order);
                        }
                        match sort_by {
                            #field_sort_declarations
                        }
                    },
                    _ => {}
                }

                let pages = if limit == 0 {
                    1
                } else {
                    (count as f64 / limit as f64).ceil() as i64
                };

                if cfg!(debug_assertions) {
                    let debug = diesel::debug_query::<diesel::pg::Pg, _>(&query_builder);
                    debug!("Filter Query: {}", debug);
                }
                

                match query_builder.load::<#ident>(&connection) {
                    Ok(vals) => Ok((vals, pages)),
                    Err(e) => {
                        error!("Failed to get {} with error '{}'", stringify!(#ident), e);
                        return Err(SqlError::DieselError(e));
                    }
                }
            }
        
            pub fn #sql_count_function_name(
                &self,
                pool: &PgPool
            ) -> Result<i64, SqlError> {

                let connection = match pool.get() {
                    Ok(connection) => connection,
                    Err(e) => {
                        error!("Failed to get pooled connection with error '{}'", e);
                        return Err(SqlError::ConnectionError);
                    }
                };

                let mut limit = self.limit.unwrap_or(100) as i64;
                if limit < 0 {
                    limit = 100;
                }

                let mut page = self.page.unwrap_or(0) as i64;
                if page < 0 {
                    page = 0;
                }

                let mut query_builder = #sql_table::table
                    .into_boxed();

                #query_builder_declarations

                match query_builder.count().get_result::<i64>(&connection) {
                    Ok(count) => Ok(count),
                    Err(e) => {
                        error!("Failed to get count of {} with error '{}'", stringify!(#ident), e);
                        return Err(SqlError::DieselError(e));
                    }
                }
            }
        }
    };

    output.into()
}
