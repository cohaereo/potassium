use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SharedString {
    Static(&'static str),
    Owned(Arc<String>),
}

impl SharedString {
    pub fn handle(&self) -> SharedString {
        match self {
            SharedString::Static(s) => SharedString::Static(s),
            SharedString::Owned(s) => SharedString::Owned(Arc::clone(s)),
        }
    }
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
