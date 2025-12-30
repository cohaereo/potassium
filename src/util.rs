use std::sync::Arc;

/// A string that can be either a static string slice or an owned string wrapped in an Arc.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SharedString {
    /// A static string slice.
    Static(&'static str),
    /// An owned string wrapped in an Arc, allowing for cheap cloning.
    Owned(Arc<String>),
}

impl From<&'static str> for SharedString {
    fn from(s: &'static str) -> Self {
        SharedString::Static(s)
    }
}

impl From<String> for SharedString {
    fn from(s: String) -> Self {
        SharedString::Owned(Arc::new(s))
    }
}

impl std::fmt::Display for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedString::Static(s) => write!(f, "{}", s),
            SharedString::Owned(s) => write!(f, "{}", s),
        }
    }
}

impl std::ops::Deref for SharedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            SharedString::Static(s) => s,
            SharedString::Owned(s) => s.as_str(),
        }
    }
}

impl std::borrow::Borrow<str> for SharedString {
    fn borrow(&self) -> &str {
        match self {
            SharedString::Static(s) => s,
            SharedString::Owned(s) => s.as_str(),
        }
    }
}
