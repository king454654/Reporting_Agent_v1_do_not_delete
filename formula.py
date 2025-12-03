# from typing import Dict, Any, Optional

def calculate_marketing_kpis(
    total_spend,
    # : float,
    total_revenue,
    # : float,
    total_budget,
    # : float,
    total_conversions,
    # : int,
    days_elapsed,
    # : int,
    total_days_in_period,
    # : int
):
#  -> Dict[str, Any]:
    """
    Calculates a set of key marketing performance indicators (KPIs) 
    based on campaign data.

    Args:
        total_spend: The total amount of money spent.
        total_revenue: The total revenue generated from the spend.
        total_budget: The total allocated budget for the period.
        total_conversions: The total number of conversions (e.g., sales, signups).
        days_elapsed: The number of days that have passed in the period.
        total_days_in_period: The total number of days in the budget period.

    Returns:
        A dictionary containing all the original inputs and calculated KPIs.
    """

    kpis = {
        # --- Inputs ---
        "total_spend": total_spend,
        "total_revenue": total_revenue,
        "total_budget": total_budget,
        "total_conversions": total_conversions,
        "days_elapsed": days_elapsed,
        "total_days_in_period": total_days_in_period,
    }

    # --- Calculated KPIs ---

    # 1. ROAS (Return on Ad Spend)
    # Formula: Total Revenue / Total Spend
    # Handles division by zero if spend is 0.
    kpis["roas"] = (total_revenue / total_spend) if total_spend > 0 else 0.0

    # 2. CPA (Cost Per Acquisition)
    # Formula: Total Spend / Total Conversions
    # Returns None if there are 0 conversions, as CPA is undefined.
    kpis["cpa"] = (total_spend / total_conversions) if total_conversions > 0 else None

    # --- Pacing Metrics ---
    
    # 3. Spend Pacing (as a percentage/decimal)
    # Formula: Total Spend / Total Budget
    spend_pacing = (total_spend / total_budget) if total_budget > 0 else None
    kpis["spend_pacing_pct"] = spend_pacing

    # 4. Expected Pacing (as a percentage/decimal)
    # Formula: Days Elapsed / Total Days in Period
    expected_pacing = (days_elapsed / total_days_in_period) if total_days_in_period > 0 else None
    kpis["expected_pacing_pct"] = expected_pacing

    # 5. Pacing Variance
    # Formula: Spend Pacing - Expected Pacing
    pacing_variance = None
    if spend_pacing is not None and expected_pacing is not None:
        pacing_variance = spend_pacing - expected_pacing
    kpis["pacing_variance_pct"] = pacing_variance

    # 6. Pacing Status
    # A qualitative status based on the variance.
    pacing_status = "N/A"
    if pacing_variance is not None:
        # We can define "On Track" as being within a 5% threshold
        if abs(pacing_variance) <= 0.05:
            pacing_status = "On Track"
        elif pacing_variance > 0.05:
            pacing_status = "Over Pacing"  # Spending faster than time
        else:
            pacing_status = "Under Pacing" # Spending slower than time
    kpis["pacing_status"] = pacing_status

    return kpis