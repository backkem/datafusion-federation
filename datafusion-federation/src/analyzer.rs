use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    datasource::source_as_provider,
    error::{DataFusionError, Result},
    logical_expr::{LogicalPlan, TableScan, TableSource},
    optimizer::analyzer::AnalyzerRule,
};

use crate::{FederatedTableProviderAdaptor, FederatedTableSource, FederationProviderRef};

#[derive(Default)]
pub struct FederationAnalyzerRule {}

impl AnalyzerRule for FederationAnalyzerRule {
    // Walk over the plan, look for the largest subtrees that only have
    // TableScans from the same FederationProvider.
    // There 'largest sub-trees' are passed to their respective FederationProvider.optimizer.
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let (optimized, _) = self.optimize_recursively(&plan, None, config)?;
        if let Some(result) = optimized {
            return Ok(result);
        }
        Ok(plan.clone())
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "federation_optimizer_rule"
    }
}

impl FederationAnalyzerRule {
    pub fn new() -> Self {
        Self::default()
    }

    // optimize_recursively recursively finds the largest sub-plans that can be federated
    // to a single FederationProvider.
    // Returns a plan if a sub-tree was federated, otherwise None.
    // Returns a FederationProvider if it covers the entire sub-tree, otherwise None.
    fn optimize_recursively(
        &self,
        plan: &LogicalPlan,
        parent: Option<&LogicalPlan>,
        _config: &ConfigOptions,
    ) -> Result<(Option<LogicalPlan>, Option<FederationProviderRef>)> {
        // Check if this node determines the FederationProvider
        let sole_provider = self.get_federation_provider(plan)?;
        if sole_provider.is_some() {
            return Ok((None, sole_provider));
        }

        // optimize_inputs
        let inputs = plan.inputs();
        if inputs.is_empty() {
            return Ok((None, None));
        }

        let (mut new_inputs, providers): (Vec<_>, Vec<_>) = inputs
            .iter()
            .map(|i| self.optimize_recursively(i, Some(plan), _config))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        // Note: assumes provider is None if ambiguous
        let first_provider = providers.first().unwrap();
        let is_singular = providers.iter().all(|p| p.is_some() && p == first_provider);

        if is_singular {
            if parent.is_none() {
                // federate the entire plan
                if let Some(provider) = first_provider {
                    if let Some(optimizer) = provider.analyzer() {
                        let optimized = optimizer.execute_and_check(plan, _config, |_, _| {})?;
                        return Ok((Some(optimized), None));
                    }
                    return Ok((None, None));
                }
                return Ok((None, None));
            }
            // The largest sub-plan is higher up.
            return Ok((None, first_provider.clone()));
        }

        // ambiguous & not federated yet: the inputs are the largest sub-plans.
        // TODO: below is WIP & untested
        if new_inputs.iter().all(|i| i.is_none()) {
            new_inputs = inputs
                .iter()
                .enumerate()
                .map(|(i, sub_plan)| {
                    if let Some(provider) = providers.get(i).unwrap() {
                        if let Some(optimizer) = provider.analyzer() {
                            let optimized =
                                optimizer.execute_and_check(sub_plan, _config, |_, _| {})?;
                            return Ok(Some(optimized));
                        }
                        return Ok(None);
                    }
                    Ok(None)
                })
                .collect::<Result<Vec<_>>>()?;
        }

        // Merge inputs
        let merged_inputs = new_inputs
            .into_iter()
            .enumerate()
            .map(|(i, o)| match o {
                Some(plan) => plan,
                None => (*(inputs.get(i).unwrap())).clone(),
            })
            .collect::<Vec<_>>();

        let new_plan = plan.with_new_inputs(&merged_inputs)?;

        Ok((Some(new_plan), None))
    }

    fn get_federation_provider(&self, plan: &LogicalPlan) -> Result<Option<FederationProviderRef>> {
        match plan {
            LogicalPlan::TableScan(TableScan { ref source, .. }) => {
                let federated_source = get_table_source(source.clone())?;
                let provider = federated_source.federation_provider();
                Ok(Some(provider))
            }
            _ => Ok(None),
        }
    }
}

pub fn get_table_source(source: Arc<dyn TableSource>) -> Result<Arc<dyn FederatedTableSource>> {
    // Unwrap TableSource
    let source = source_as_provider(&source)?;

    // Get FederatedTableProviderAdaptor
    let wrapper = source
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
        .ok_or(DataFusionError::Plan(
            "expected a FederatedTableSourceWrapper".to_string(),
        ))?;

    // Return original FederatedTableSource
    Ok(wrapper.source.clone())
}
