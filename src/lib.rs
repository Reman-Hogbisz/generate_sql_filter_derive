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

#[proc_macro_derive(CreateFilter, attributes(filter_name))]
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

    let enum_name = Ident::new(&format!("{}SortBy", ident.to_string()), Span::call_site());

    let mut filtered_field_declarations = TokenStream2::default();

    let mut field_sort_by_enum_declarations = TokenStream2::default();

    const TYPES_THAT_HAVE_ORDERING: &[&'static str] = &[
        "f64", "i64", "u64", "f32", "i32", "u32", "i16", "u16", "i8", "u8", "usize", "isize",
    ];

    const TYPES_WITH_PARTIAL_EQ: &[&'static str] = &[
        "f64", "i64", "u64", "f32", "i32", "u32", "i16", "u16", "i8", "u8", "usize", "isize",
        "String", "bool", "char", "Uuid",
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
                                let field_not_contains = Ident::new(
                                    &format!("{}_not_contains", field),
                                    Span::call_site(),
                                );
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
                                let field_contains_insensitive = Ident::new(
                                    &format!("{}_contains_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_not_contains_insensitive = Ident::new(
                                    &format!("{}_not_contains_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_starts_with_insensitive = Ident::new(
                                    &format!("{}_starts_with_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_not_starts_with_insensitive = Ident::new(
                                    &format!("{}_not_starts_with_insensitive", field),
                                    Span::call_site(),
                                );
                                let field_ends_with_insensitive = Ident::new(
                                    &format!("{}_ends_with_insensitive", field),
                                    Span::call_site(),
                                );
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
                            }
                            _ => (),
                        }

                        if TYPES_THAT_HAVE_ORDERING.contains(&ident_str)
                            || t.to_token_stream().into_iter().any(|token| match token {
                                proc_macro2::TokenTree::Ident(ident) => {
                                    TYPES_THAT_HAVE_ORDERING.contains(&ident.to_string().as_str())
                                }
                                _ => false,
                            })
                        {
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

                            let sort_by_field_name = to_camel_case(field.to_string());
                            let sort_by_field = Ident::new(&sort_by_field_name, Span::call_site());
                            field_sort_by_enum_declarations.extend::<TokenStream2>(quote! {
                                #sort_by_field,
                            });

                            return;
                        } else if ftype
                            .to_token_stream()
                            .into_iter()
                            .any(|token| match token {
                                proc_macro2::TokenTree::Ident(ident) => {
                                    // NaiveDate/Time, Date/Time<_>
                                    ident.to_string().contains("Date")
                                }
                                _ => false,
                            })
                        {
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
                        } else {
                            return;
                        }
                    }
                }
                _ => {
                    return;
                }
            }
            let field_in = Ident::new(&format!("{}_in", field), Span::call_site());
            let field_not_in = Ident::new(&format!("{}_not_in", field), Span::call_site());
            let field_not = Ident::new(&format!("{}_not", field), Span::call_site());
            filtered_field_declarations.extend::<TokenStream2>(quote! {
                pub #field : Option<#ftype>,
                pub #field_not : Option<#ftype>,
                pub #field_in : Option<Vec<#ftype>>,
                pub #field_not_in : Option<Vec<#ftype>>,
            });

            let sort_by_field_name = to_camel_case(field.to_string());
            let sort_by_field = Ident::new(&sort_by_field_name, Span::call_site());
            field_sort_by_enum_declarations.extend::<TokenStream2>(quote! {
                #sort_by_field,
            });
        });

    let output = quote! {

        use crate::util::*;

        #[derive(Clone, Copy, Debug, Serialize, PartialEq)]
        pub enum #enum_name {
            #field_sort_by_enum_declarations
        }

        #[derive(Default, Clone, Debug, Serialize, PartialEq)]
        pub struct #struct_name {
            pub limit: Option<i32>,
            pub page: Option<i32>,
            pub sort_by: Option<#enum_name>,
            pub sort_order: Option<FilterSortOrder>,
            #filtered_field_declarations
        }
    };

    output.into()
}
