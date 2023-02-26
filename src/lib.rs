use proc_macro::{self, TokenStream};
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, DeriveInput, FieldsNamed};

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

    let mut filtered_field_declarations = TokenStream2::default();

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
                                let field_starts_with = Ident::new(
                                    &format!("{}_starts_with", field),
                                    Span::call_site(),
                                );
                                let field_ends_with =
                                    Ident::new(&format!("{}_ends_with", field), Span::call_site());
                                filtered_field_declarations.extend::<TokenStream2>(quote! {
                                    pub #field_contains : Option<String>,
                                    pub #field_starts_with : Option<String>,
                                    pub #field_ends_with : Option<String>,
                                });

                                query_builder_declarations.extend::<TokenStream2>(quote! {
                                    if let Some(contains) = self.#field_contains.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("%{}%", contains)));
                                    }

                                    if let Some(starts_with) = self.#field_starts_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("{}%", starts_with)));
                                    }

                                    if let Some(ends_with) = self.#field_ends_with.as_ref() {
                                        query_builder = query_builder.filter(#sql_table::#field.like(format!("%{}", ends_with)));
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
                            let field_is_none =
                                Ident::new(&format!("{}_is_none", field), Span::call_site());

                            filtered_field_declarations.extend::<TokenStream2>(quote! {
                                pub #field_is_some : #ftype,
                                pub #field_is_none : Option<bool>,
                            });

                            query_builder_declarations.extend::<TokenStream2>(quote! {
                                if let Some(is_some) = &self.#field_is_some {
                                    query_builder = query_builder.filter(#sql_table::#field.eq(is_some));
                                }

                                if let Some(is_none) = self.#field_is_none {
                                    if is_none {
                                        query_builder = query_builder.filter(#sql_table::#field.is_null());
                                    } else {
                                        query_builder = query_builder.filter(#sql_table::#field.is_not_null());
                                    }
                                }
                            });
                        
                            return;
                        }
                        else if ftype
                        .to_token_stream()
                        .into_iter()
                        .any(|token| match token {
                            proc_macro2::TokenTree::Ident(ident) => {
                                ident.to_string().as_str() == "NaiveDateTime"
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
            filtered_field_declarations.extend::<TokenStream2>(quote! { pub #field : Option<#ftype>, });
            query_builder_declarations.extend::<TokenStream2>(quote! {
                if let Some(value) = &self.#field {
                    query_builder = query_builder.filter(#sql_table::#field.eq(value));
                }
            });
        });

    let sql_filter_function_name = Ident::new(
        &format!("filter_{}", ident.to_string().to_lowercase()),
        Span::call_site(),
    );

    let output = quote! {

        use crate::util::*;
        use crate::db_connection::*;
        use diesel::prelude::*;

        #[derive(Default, Clone, Debug, Deserialize, PartialEq)]
        pub struct #struct_name {
            pub limit: Option<i64>,
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

                let mut query_builder = #sql_table::table
                    .limit(self.limit.unwrap_or(100))
                    .into_boxed();

                #query_builder_declarations

                match query_builder.load::<#ident>(&connection) {
                    Ok(vals) => Ok(vals),
                    Err(e) => {
                        error!("Failed to get {} with error '{}'", stringify!(#ident), e);
                        return Err(SqlError::DieselError(e));
                    }
                }
            }

        }
    };

    output.into()
}
