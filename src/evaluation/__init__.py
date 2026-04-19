from __future__ import annotations

from src.evaluation.base import BaseEvaluator, EVALUATION_REQUIRED_ARTIFACTS, EvaluationContext
from src.evaluation.correlation import build_factor_correlation_tables
from src.evaluation.fama_macbeth import build_fama_macbeth_tables
from src.evaluation.ic import RankICEvaluator, build_evaluation_input, build_rank_ic_tables
from src.evaluation.portfolio import build_quantile_portfolio_tables
from src.evaluation.redundancy import build_redundancy_tables
from src.evaluation.robustness import build_subperiod_robustness_tables
from src.evaluation.summary import build_evaluation_summary, build_monotonicity_summary

__all__ = [
    "BaseEvaluator",
    "EVALUATION_REQUIRED_ARTIFACTS",
    "EvaluationContext",
    "RankICEvaluator",
    "build_factor_correlation_tables",
    "build_fama_macbeth_tables",
    "build_evaluation_input",
    "build_rank_ic_tables",
    "build_quantile_portfolio_tables",
    "build_redundancy_tables",
    "build_subperiod_robustness_tables",
    "build_monotonicity_summary",
    "build_evaluation_summary",
]
