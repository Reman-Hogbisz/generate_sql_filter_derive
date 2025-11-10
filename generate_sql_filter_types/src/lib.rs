#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use ts_rs::TS;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, TS), ts(export))]
pub enum Operator {
    #[cfg_attr(feature = "serde", serde(rename = "is"))]
    Is,
    #[cfg_attr(feature = "serde", serde(rename = "is_not"))]
    IsNot,
    #[cfg_attr(feature = "serde", serde(rename = "is_any_of"))]
    IsAnyOf,
    #[cfg_attr(feature = "serde", serde(rename = "is_not_any_of"))]
    IsNotAnyOf,
    #[cfg_attr(feature = "serde", serde(rename = "includes_all"))]
    IncludesAll,
    #[cfg_attr(feature = "serde", serde(rename = "excludes_all"))]
    ExcludesAll,
    #[cfg_attr(feature = "serde", serde(rename = "before"))]
    Before,
    #[cfg_attr(feature = "serde", serde(rename = "after"))]
    After,
    #[cfg_attr(feature = "serde", serde(rename = "between"))]
    Between,
    #[cfg_attr(feature = "serde", serde(rename = "not_between"))]
    NotBetween,
    #[cfg_attr(feature = "serde", serde(rename = "contains"))]
    Contains,
    #[cfg_attr(feature = "serde", serde(rename = "not_contains"))]
    NotContains,
    #[cfg_attr(feature = "serde", serde(rename = "contains_case_sensitive"))]
    ContainsCaseSensitive,
    #[cfg_attr(feature = "serde", serde(rename = "not_contains_case_sensitive"))]
    NotContainsCaseSensitive,
    #[cfg_attr(feature = "serde", serde(rename = "starts_with"))]
    StartsWith,
    #[cfg_attr(feature = "serde", serde(rename = "ends_with"))]
    EndsWith,
    #[cfg_attr(feature = "serde", serde(rename = "starts_with_case_sensitive"))]
    StartsWithCaseSensitive,
    #[cfg_attr(feature = "serde", serde(rename = "ends_with_case_sensitive"))]
    EndsWithCaseSensitive,
    #[cfg_attr(feature = "serde", serde(rename = "greater_than"))]
    GreaterThan,
    #[cfg_attr(feature = "serde", serde(rename = "less_than"))]
    LessThan,
    #[cfg_attr(feature = "serde", serde(rename = "overlaps"))]
    Overlaps,
    #[cfg_attr(feature = "serde", serde(rename = "includes"))]
    Includes,
    #[cfg_attr(feature = "serde", serde(rename = "excludes"))]
    Excludes,
    #[cfg_attr(feature = "serde", serde(rename = "empty"))]
    Empty,
    #[cfg_attr(feature = "serde", serde(rename = "not_empty"))]
    NotEmpty,
    #[cfg_attr(feature = "serde", serde(rename = "like"))]
    Like,
    #[cfg_attr(feature = "serde", serde(rename = "not_like"))]
    NotLike,
}
