use core::fmt;
use std::sync::Arc;

use datafusion::optimizer::analyzer::Analyzer;

mod analyzer;
pub use analyzer::*;
mod table_provider;
pub use table_provider::*;

pub type FederationProviderRef = Arc<dyn FederationProvider>;
pub trait FederationProvider: Send + Sync {
    // Returns the name of the provider, used for comparison.
    fn name(&self) -> &str;

    // Returns the compute context in which this federation provider
    // will execute a query. For example: database instance & catalog.
    fn compute_context(&self) -> Option<String>;

    // Returns an analyzer that can cut out part of the plan
    // to federate it.
    fn analyzer(&self) -> Option<Arc<Analyzer>>;
}

impl fmt::Display for dyn FederationProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {:?}", self.name(), self.compute_context())
    }
}

impl PartialEq<dyn FederationProvider> for dyn FederationProvider {
    /// Comparing name, args and return_type
    fn eq(&self, other: &dyn FederationProvider) -> bool {
        self.name() == other.name() && self.compute_context() == other.compute_context()
    }
}

impl Eq for dyn FederationProvider {}
